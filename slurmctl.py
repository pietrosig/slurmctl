#!/bin/sh
""":'
for py in "${SLURMCTL_PYTHON:-}" python3.13 python3.12 python3.11 python3; do
    [ -n "$py" ] || continue
    "$py" -c "import sys; raise SystemExit(sys.version_info < (3, 11)" >/dev/null 2>&1 || continue
    exec "$py" "$0" "$@"
done
echo "slurmctl: Python 3.11+ is required" >&2
exit 127
':"""

from __future__ import annotations

import argparse
import curses
import datetime as dt
import errno
import getpass
import json
import os
import queue
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path


WRAPPER_CODE = r"""from __future__ import annotations

import datetime as _dt
import fcntl
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path


OPTIONS_WITH_VALUES = {
    "--output": "output",
    "-o": "output",
    "--error": "error",
    "-e": "error",
    "--job-name": "job_name",
    "-J": "job_name",
    "--array": "array",
    "-a": "array",
    "--chdir": "chdir",
}

NO_SCRIPT_LONG_OPTIONS = {
    "--help",
    "--version",
    "--verbose",
    "--quiet",
    "--parsable",
}


def utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, sort_keys=True) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def write_json(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def parse_sbatch_args(args: list[str]) -> tuple[dict, str | None, list[str]]:
    parsed: dict[str, object] = {}
    script = None
    script_args: list[str] = []
    i = 0
    end_options = False

    while i < len(args):
        arg = args[i]
        if script is not None:
            script_args.append(arg)
            i += 1
            continue

        if end_options:
            script = arg
            i += 1
            continue

        if arg == "--":
            end_options = True
            i += 1
            continue

        if arg.startswith("--"):
            name, sep, value = arg.partition("=")
            if name in OPTIONS_WITH_VALUES:
                key = OPTIONS_WITH_VALUES[name]
                if sep:
                    parsed[key] = value
                    i += 1
                elif i + 1 < len(args):
                    parsed[key] = args[i + 1]
                    i += 2
                else:
                    parsed[key] = None
                    i += 1
                continue
            if name in NO_SCRIPT_LONG_OPTIONS:
                parsed.setdefault("flags", []).append(arg)
                i += 1
                continue
            parsed.setdefault("other_options", []).append(arg)
            i += 1
            continue

        if arg in OPTIONS_WITH_VALUES:
            key = OPTIONS_WITH_VALUES[arg]
            if i + 1 < len(args):
                parsed[key] = args[i + 1]
                i += 2
            else:
                parsed[key] = None
                i += 1
            continue

        if arg.startswith("-") and arg != "-":
            parsed.setdefault("other_options", []).append(arg)
            i += 1
            continue

        script = arg
        i += 1

    if script_args:
        parsed["script_args"] = script_args
    return parsed, script, script_args


def parse_directive_tokens(tokens: list[str]) -> dict:
    parsed: dict[str, object] = {}
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.startswith("--"):
            name, sep, value = token.partition("=")
            if name in OPTIONS_WITH_VALUES:
                key = OPTIONS_WITH_VALUES[name]
                if sep:
                    parsed[key] = value
                    i += 1
                elif i + 1 < len(tokens):
                    parsed[key] = tokens[i + 1]
                    i += 2
                else:
                    parsed[key] = None
                    i += 1
                continue
        if token in OPTIONS_WITH_VALUES:
            key = OPTIONS_WITH_VALUES[token]
            if i + 1 < len(tokens):
                parsed[key] = tokens[i + 1]
                i += 2
            else:
                parsed[key] = None
                i += 1
            continue
        parsed.setdefault("other_directives", []).append(token)
        i += 1
    return parsed


def parse_script_directives(script_path: str | None, submit_cwd: Path) -> dict:
    if not script_path:
        return {}
    path = Path(script_path)
    if not path.is_absolute():
        path = submit_cwd / path
    if not path.exists():
        return {"_error": f"submitted script not found: {script_path}"}

    parsed: dict[str, object] = {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("#SBATCH"):
                    body = stripped[len("#SBATCH"):].strip()
                    if body:
                        parsed.update(parse_directive_tokens(shlex.split(body)))
                    continue
                if stripped.startswith("#"):
                    continue
                break
    except OSError as exc:
        return {"_error": str(exc)}
    return parsed


def normalize_options(script_directives: dict, cli_options: dict) -> dict:
    normalized = dict(script_directives)
    normalized.update({k: v for k, v in cli_options.items() if k not in {"script_args"}})
    normalized.setdefault("output", "slurm-%j.out")
    normalized.setdefault("error", normalized.get("output"))
    return normalized


def infer_job_name(job_name: str | None, script_path: str | None) -> str | None:
    if job_name:
        return str(job_name)
    if script_path:
        return Path(script_path).name
    return None


def resolve_template(
    template: str | None,
    job_id: str | None,
    job_name: str | None,
    script_path: str | None = None,
) -> str | None:
    if template is None:
        return None
    result = str(template)
    job_name = infer_job_name(job_name, script_path)
    if job_id:
        result = result.replace("%j", job_id)
        result = result.replace("%J", job_id)
        result = result.replace("%A", job_id.split("_", 1)[0])
        if "_" in job_id:
            result = result.replace("%a", job_id.split("_", 1)[1])
    if job_name:
        result = result.replace("%x", job_name)
    result = result.replace("%%", "%")
    return result


def extract_job_id(stdout: str, stderr: str) -> str | None:
    for text in (stdout, stderr):
        match = re.search(r"Submitted batch job\s+(\d+)", text)
        if match:
            return match.group(1)
        match = re.search(r"\b(\d+)(?:_[^\s]+)?\b", text.strip())
        if match:
            return match.group(1)
    return None


def stable_hash(payload: dict) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]


def main() -> int:
    args = sys.argv[1:]
    submit_cwd = Path.cwd().resolve()
    slurmctl_dir = Path(os.environ["SLURMCTL_DIR"]).resolve()
    dry_run = os.environ.get("SLURMCTL_DRY_RUN") == "1"
    verbose = os.environ.get("SLURMCTL_VERBOSE") == "1"
    real_sbatch = os.environ.get("SLURMCTL_REAL_SBATCH")

    parsed_cli, script_path, _script_args = parse_sbatch_args(args)
    parsed_script = parse_script_directives(script_path, submit_cwd)
    normalized = normalize_options(parsed_script, parsed_cli)

    script_abs = None
    if script_path:
        candidate = Path(script_path)
        script_abs = str(candidate if candidate.is_absolute() else (submit_cwd / candidate).resolve())

    command_payload = {
        "submit_cwd": str(submit_cwd),
        "script_path": script_path,
        "script_abs_path": script_abs,
        "raw_args": args,
        "parsed_cli_options": parsed_cli,
        "parsed_script_directives": parsed_script,
        "normalized_options": normalized,
    }
    command_hash = stable_hash(command_payload)

    if verbose:
        print(f"slurmctl: intercepted sbatch {' '.join(shlex.quote(a) for a in args)}", file=sys.stderr)

    if dry_run:
        sbatch_stdout = "Submitted batch job 999999\n"
        sbatch_stderr = ""
        returncode = 0
    else:
        if not real_sbatch:
            print("slurmctl: SLURMCTL_REAL_SBATCH is not set; use --dry-run if sbatch is unavailable", file=sys.stderr)
            return 127
        proc = subprocess.run([real_sbatch, *args], text=True, capture_output=True)
        sbatch_stdout = proc.stdout
        sbatch_stderr = proc.stderr
        returncode = proc.returncode

    if sbatch_stdout:
        print(sbatch_stdout, end="")
    if sbatch_stderr:
        print(sbatch_stderr, end="", file=sys.stderr)

    job_id = extract_job_id(sbatch_stdout, sbatch_stderr)
    submitted_at = utc_now()
    run_id_seed = {
        "command_hash": command_hash,
        "job_id": job_id,
        "submitted_at": submitted_at,
        "pid": os.getpid(),
        "args": args,
    }
    run_id = stable_hash(run_id_seed)

    stdout_template = normalized.get("output")
    stderr_template = normalized.get("error")
    job_name = normalized.get("job_name")
    resolved_stdout = resolve_template(stdout_template, job_id, job_name, script_path)
    resolved_stderr = resolve_template(stderr_template, job_id, job_name, script_path)

    run_record = {
        "run_id": run_id,
        "command_hash": command_hash,
        "job_id": job_id,
        "submitted_at": submitted_at,
        "submit_cwd": str(submit_cwd),
        "script_path": script_path,
        "script_abs_path": script_abs,
        "raw_args": args,
        "normalized_options": normalized,
        "stdout_template": stdout_template,
        "stderr_template": stderr_template,
        "resolved_stdout_path": resolved_stdout,
        "resolved_stderr_path": resolved_stderr,
        "sbatch_stdout": sbatch_stdout,
        "sbatch_stderr": sbatch_stderr,
        "sbatch_returncode": returncode,
    }

    command_path = slurmctl_dir / "commands" / f"{command_hash}.json"
    if command_path.exists():
        try:
            command_record = json.loads(command_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            command_record = {}
        command_record.setdefault("runs", [])
        if run_id not in command_record["runs"]:
            command_record["runs"].append(run_id)
    else:
        command_record = {
            "command_hash": command_hash,
            "submit_cwd": str(submit_cwd),
            "script_path": script_path,
            "script_abs_path": script_abs,
            "raw_args": args,
            "parsed_cli_options": parsed_cli,
            "parsed_script_directives": parsed_script,
            "normalized_options": normalized,
            "first_seen_at": submitted_at,
            "runs": [run_id],
        }

    write_json(command_path, command_record)
    write_json(slurmctl_dir / "runs" / f"{run_id}.json", run_record)
    append_jsonl(slurmctl_dir / "submissions.jsonl", run_record)

    if verbose:
        print(f"slurmctl: wrote {slurmctl_dir / 'runs' / (run_id + '.json')}", file=sys.stderr)
        print(f"slurmctl: wrote {command_path}", file=sys.stderr)

    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
"""


DEFAULT_SETTINGS = {
    "editor": "",
    "slurmctl_dir": ".slurmctl",
}

RUNNING_STATES = {
    "CONFIGURING",
    "COMPLETING",
    "RESIZING",
    "RUNNING",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",
    "SUSPENDED",
}
PENDING_STATES = {
    "PENDING",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "REQUEUED",
    "RESV_DEL_HOLD",
    "REVOKED",
    "SPECIAL_EXIT",
}
FINISHED_STATES = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "TIMEOUT",
}


@dataclass(frozen=True)
class WatchRefreshResult:
    token: int
    jobs: list[dict]
    source: str
    loaded_at: float
    error: str = ""
    finished_jobs: list[dict] | None = None
    finished_source: str = ""
    finished_loaded_at: float = 0.0


def infer_output_job_name(job_name: object, script_path: object) -> str | None:
    if job_name:
        return str(job_name)
    if script_path:
        return Path(str(script_path)).name
    return None


def resolve_output_template(
    template: object,
    job_id: object,
    job_name: object,
    script_path: object,
) -> str | None:
    if template is None:
        return None
    result = str(template)
    job_id_text = str(job_id or "")
    inferred_job_name = infer_output_job_name(job_name, script_path)
    if job_id_text:
        result = result.replace("%j", job_id_text)
        result = result.replace("%J", job_id_text)
        result = result.replace("%A", job_id_text.split("_", 1)[0])
        if "_" in job_id_text:
            result = result.replace("%a", job_id_text.split("_", 1)[1])
    if inferred_job_name:
        result = result.replace("%x", inferred_job_name)
    result = result.replace("%%", "%")
    return result


def config_path() -> Path:
    config_home = os.environ.get("XDG_CONFIG_HOME")
    if config_home:
        return Path(config_home) / "slurmctl" / "settings.json"
    return Path.home() / ".config" / "slurmctl" / "settings.json"


def load_settings() -> dict:
    path = config_path()
    settings = dict(DEFAULT_SETTINGS)
    if not path.exists():
        return settings
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except OSError, json.JSONDecodeError:
        return settings
    if isinstance(loaded, dict):
        settings.update(
            {key: str(value) for key, value in loaded.items() if key in settings}
        )
    return settings


def save_settings(settings: dict) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = dict(DEFAULT_SETTINGS)
    data.update({key: str(value) for key, value in settings.items() if key in data})
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def extract_tool_options(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--slurmctl-dir")
    parser.add_argument("-h", "--help", action="store_true")
    known, remaining = parser.parse_known_args(argv)
    return known, remaining


def find_sbatch_command(argv: list[str]) -> int | None:
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in {"--dry-run", "--verbose"}:
            i += 1
            continue
        if arg == "--slurmctl-dir":
            i += 2
            continue
        if arg.startswith("--slurmctl-dir="):
            i += 1
            continue
        if arg in {"-h", "--help"}:
            return None
        return i if arg == "sbatch" else None
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slurmctl.py",
        description="Minimal sbatch interception helper.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="simulate sbatch submissions"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="print intercepted calls and paths"
    )
    parser.add_argument("--slurmctl-dir", help="metadata directory")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser(
        "run", help="run a script with sbatch interception"
    )
    run_parser.add_argument("script")
    run_parser.add_argument("script_args", nargs=argparse.REMAINDER)

    sbatch_parser = subparsers.add_parser(
        "sbatch", help="submit one sbatch recipe and capture it"
    )
    sbatch_parser.add_argument("sbatch_args", nargs=argparse.REMAINDER)

    subparsers.add_parser("show", help="show captured submissions")
    subparsers.add_parser("watch", help="show current Slurm jobs")
    subparsers.add_parser("doctor", help="check sbatch discovery")
    subparsers.add_parser(
        "interactive", aliases=["shell", "ui"], help="open interactive shell"
    )
    return parser


def discover_real_sbatch() -> str | None:
    return shutil.which("sbatch", path=os.environ.get("PATH", ""))


def make_wrapper(wrapper_dir: Path) -> Path:
    wrapper = wrapper_dir / "sbatch"
    python = sys.executable or shutil.which("python3") or "python3"
    launcher = (
        """#!/bin/sh
''':'
py="${SLURMCTL_PYTHON:-%s}"
"$py" -c "import sys; raise SystemExit(sys.version_info < (3, 11))" >/dev/null 2>&1 || {
    echo "slurmctl sbatch wrapper: Python 3.11+ is required" >&2
    exit 127
}
exec "$py" "$0" "$@"
':'''
"""
        % python
    )
    wrapper.write_text(f"{launcher}{WRAPPER_CODE}", encoding="utf-8")
    mode = wrapper.stat().st_mode
    wrapper.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return wrapper


def run_script(args: argparse.Namespace) -> int:
    script = Path(args.script)
    script_abs = script if script.is_absolute() else (Path.cwd() / script).resolve()
    if not script_abs.exists():
        print(f"slurmctl: script not found: {args.script}", file=sys.stderr)
        return 2

    real_sbatch = discover_real_sbatch()
    if not real_sbatch and not args.dry_run:
        print(
            "slurmctl: sbatch not found on PATH; rerun with --dry-run to simulate submissions",
            file=sys.stderr,
        )
        return 127

    slurmctl_dir = Path(args.slurmctl_dir)
    slurmctl_dir_abs = (
        slurmctl_dir
        if slurmctl_dir.is_absolute()
        else (Path.cwd() / slurmctl_dir).resolve()
    )
    slurmctl_dir_abs.mkdir(parents=True, exist_ok=True)
    (slurmctl_dir_abs / "commands").mkdir(exist_ok=True)
    (slurmctl_dir_abs / "runs").mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="slurmctl-") as tmp:
        wrapper_dir = Path(tmp)
        make_wrapper(wrapper_dir)
        env = os.environ.copy()
        env["PATH"] = f"{wrapper_dir}{os.pathsep}{env.get('PATH', '')}"
        env["SLURMCTL_DIR"] = str(slurmctl_dir_abs)
        env["SLURMCTL_DRY_RUN"] = "1" if args.dry_run else "0"
        env["SLURMCTL_VERBOSE"] = "1" if args.verbose else "0"
        env["SLURMCTL_PYTHON"] = sys.executable
        if real_sbatch:
            env["SLURMCTL_REAL_SBATCH"] = real_sbatch

        if args.verbose:
            print(
                f"slurmctl: real sbatch: {real_sbatch or '(not found)'}",
                file=sys.stderr,
            )
            print(f"slurmctl: wrapper dir: {wrapper_dir}", file=sys.stderr)
            print(f"slurmctl: metadata dir: {slurmctl_dir_abs}", file=sys.stderr)

        command = [str(script_abs), *args.script_args]
        if not os.access(script_abs, os.X_OK):
            command = ["/bin/sh", str(script_abs), *args.script_args]
        try:
            proc = subprocess.run(command, env=env)
        except OSError as exc:
            if exc.errno != errno.ENOEXEC or command[0] == "/bin/sh":
                raise
            proc = subprocess.run(
                ["/bin/sh", str(script_abs), *args.script_args], env=env
            )
        return proc.returncode


def run_sbatch_args(
    raw_args: list[str],
    *,
    submit_cwd: str | None,
    slurmctl_dir: str,
    dry_run: bool,
    verbose: bool,
) -> int:
    """Run one sbatch recipe through the same fake wrapper used by run."""
    real_sbatch = discover_real_sbatch()
    if not real_sbatch and not dry_run:
        print(
            "slurmctl: sbatch not found on PATH; rerun with --dry-run to simulate submissions",
            file=sys.stderr,
        )
        return 127

    slurmctl_dir_abs = Path(slurmctl_dir)
    if not slurmctl_dir_abs.is_absolute():
        slurmctl_dir_abs = (Path.cwd() / slurmctl_dir_abs).resolve()
    slurmctl_dir_abs.mkdir(parents=True, exist_ok=True)
    (slurmctl_dir_abs / "commands").mkdir(exist_ok=True)
    (slurmctl_dir_abs / "runs").mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="slurmctl-") as tmp:
        wrapper_dir = Path(tmp)
        wrapper = make_wrapper(wrapper_dir)
        env = os.environ.copy()
        env["PATH"] = f"{wrapper_dir}{os.pathsep}{env.get('PATH', '')}"
        env["SLURMCTL_DIR"] = str(slurmctl_dir_abs)
        env["SLURMCTL_DRY_RUN"] = "1" if dry_run else "0"
        env["SLURMCTL_VERBOSE"] = "1" if verbose else "0"
        env["SLURMCTL_PYTHON"] = sys.executable
        if real_sbatch:
            env["SLURMCTL_REAL_SBATCH"] = real_sbatch
        cwd = submit_cwd if submit_cwd and Path(submit_cwd).exists() else None
        proc = subprocess.run([str(wrapper), *raw_args], cwd=cwd, env=env)
        return proc.returncode


def sbatch(args: argparse.Namespace) -> int:
    if not args.sbatch_args:
        print(
            "slurmctl: usage: slurmctl sbatch [SBATCH_OPTIONS...] SCRIPT [SCRIPT_ARGS...]",
            file=sys.stderr,
        )
        return 2
    return run_sbatch_args(
        list(args.sbatch_args),
        submit_cwd=str(Path.cwd()),
        slurmctl_dir=args.slurmctl_dir,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


def load_submissions(slurmctl_dir: str) -> list[dict]:
    submissions = Path(slurmctl_dir) / "submissions.jsonl"
    if not submissions.exists():
        return []
    records = []
    with submissions.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def rewrite_submissions(slurmctl_dir: str, records: list[dict]) -> None:
    slurmctl_path = Path(slurmctl_dir)
    slurmctl_path.mkdir(parents=True, exist_ok=True)
    submissions = slurmctl_path / "submissions.jsonl"
    submissions.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def delete_submission(record: dict, slurmctl_dir: str) -> None:
    records = [
        item
        for item in load_submissions(slurmctl_dir)
        if item.get("run_id") != record.get("run_id")
    ]
    rewrite_submissions(slurmctl_dir, records)
    run_id = record.get("run_id")
    if run_id:
        run_path = Path(slurmctl_dir) / "runs" / f"{run_id}.json"
        try:
            run_path.unlink()
        except FileNotFoundError:
            pass

    command_hash = record.get("command_hash")
    if not command_hash or not run_id:
        return
    command_path = Path(slurmctl_dir) / "commands" / f"{command_hash}.json"
    if not command_path.exists():
        return
    try:
        command = json.loads(command_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return
    command["runs"] = [item for item in command.get("runs", []) if item != run_id]
    command_path.write_text(
        json.dumps(command, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def editor_command(configured: str | None = None) -> list[str]:
    configured = configured or os.environ.get("VISUAL") or os.environ.get("EDITOR")
    if configured:
        return shlex.split(configured)
    for candidate in ("editor", "nano", "vi"):
        path = shutil.which(candidate)
        if path:
            return [path]
    return ["vi"]


def resolve_from_cwd(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def has_slurmctl_data(path: Path) -> bool:
    return (
        (path / "submissions.jsonl").exists()
        or (path / "runs").exists()
        or (path / "commands").exists()
    )


def move_slurmctl_data(old_dir: Path, new_dir: Path) -> None:
    new_dir.mkdir(parents=True, exist_ok=True)
    for name in ("submissions.jsonl", "runs", "commands"):
        source = old_dir / name
        target = new_dir / name
        if not source.exists():
            continue
        if target.exists():
            raise FileExistsError(f"target already exists: {target}")
        shutil.move(str(source), str(target))
    try:
        old_dir.rmdir()
    except OSError:
        pass


def show(args: argparse.Namespace) -> int:
    slurmctl_dir = Path(args.slurmctl_dir)
    submissions = slurmctl_dir / "submissions.jsonl"
    if not submissions.exists():
        print(f"No submissions found in {slurmctl_dir}")
        return 0

    print(
        "JOB_ID   COMMAND_HASH      SCRIPT_PATH          STDOUT                 STDERR"
    )
    with submissions.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            record = json.loads(line)
            print(
                f"{record.get('job_id') or '-':<8} "
                f"{record.get('command_hash') or '-':<17} "
                f"{record.get('script_path') or '-':<20} "
                f"{record.get('resolved_stdout_path') or '-':<22} "
                f"{record.get('resolved_stderr_path') or '-'}"
            )
    return 0


def normalize_state(state: object) -> str:
    parts = str(state or "").strip().upper().split()
    return parts[0] if parts else ""


def job_group(job: dict) -> str:
    state = normalize_state(job.get("state"))
    if state in RUNNING_STATES:
        return "running"
    if state in PENDING_STATES:
        return "pending"
    if state in FINISHED_STATES:
        return "finished"
    return "other"


def parse_elapsed_seconds(elapsed: object) -> int:
    text = str(elapsed or "").strip()
    if not text or text in {"-", "Unknown"}:
        return 0
    days = 0
    if "-" in text:
        day_text, text = text.split("-", 1)
        try:
            days = int(day_text)
        except ValueError:
            days = 0
    parts = text.split(":")
    try:
        values = [int(part) for part in parts]
    except ValueError:
        return 0
    if len(values) == 3:
        hours, minutes, seconds = values
    elif len(values) == 2:
        hours = 0
        minutes, seconds = values
    elif len(values) == 1:
        hours = 0
        minutes = values[0]
        seconds = 0
    else:
        return 0
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def parse_job_id_number(job_id: object) -> int:
    text = str(job_id or "")
    digits = []
    for char in text:
        if char.isdigit():
            digits.append(char)
        elif digits:
            break
    if not digits:
        return -1
    return int("".join(digits))


def watch_sort_key(job: dict) -> tuple[int, int, str, str]:
    order = {"running": 0, "pending": 1, "finished": 2, "other": 3}
    group = job_group(job)
    job_id = str(job.get("job_id") or "")
    if group == "finished":
        return (order[group], -parse_job_id_number(job_id), "", job_id)
    elapsed = parse_elapsed_seconds(job.get("elapsed"))
    elapsed_key = elapsed if group == "running" else 0
    return (
        order[group],
        elapsed_key,
        str(job.get("job_name") or ""),
        job_id,
    )


def sort_watch_jobs(jobs: list[dict]) -> list[dict]:
    return sorted(jobs, key=watch_sort_key)


def watch_stats(jobs: list[dict]) -> dict[str, int]:
    stats = {"running": 0, "pending": 0, "finished": 0}
    for job in jobs:
        group = job_group(job)
        if group in stats:
            stats[group] += 1
    return stats


def parse_slurm_datetime(value: object) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text or text in {"Unknown", "N/A"}:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def load_recent_finished_jobs(
    timeout_seconds: float | None = None, *, hours: int = 24
) -> tuple[list[dict], str]:
    sacct = shutil.which("sacct")
    if not sacct:
        return [], "sacct not found"

    cutoff = dt.datetime.now() - dt.timedelta(hours=hours)
    starttime = cutoff.strftime("%Y-%m-%dT%H:%M:%S")
    command = [
        sacct,
        "-X",
        "--noheader",
        "--parsable2",
        "--user",
        getpass.getuser(),
        f"--starttime={starttime}",
        "--format=JobIDRaw,JobName,State,Elapsed,NNodes,End",
    ]
    try:
        proc = subprocess.run(
            command, text=True, capture_output=True, timeout=timeout_seconds
        )
    except subprocess.TimeoutExpired:
        timeout_text = (
            f"{timeout_seconds:g}s"
            if timeout_seconds is not None
            else "unknown timeout"
        )
        return [], f"sacct timed out after {timeout_text}"
    if proc.returncode != 0:
        return [], proc.stderr.strip() or "sacct failed"

    jobs = []
    for line in proc.stdout.splitlines():
        parts = line.split("|", 5)
        if len(parts) != 6:
            continue
        job_id, name, state, elapsed, nodes, ended_at = [part.strip() for part in parts]
        if normalize_state(state) not in FINISHED_STATES:
            continue
        parsed_end = parse_slurm_datetime(ended_at)
        if parsed_end is None or parsed_end < cutoff:
            continue
        jobs.append(
            {
                "source": "sacct",
                "job_id": job_id,
                "job_name": name,
                "state": state,
                "elapsed": elapsed,
                "nodes": nodes or "-",
                "reason": f"ended {ended_at}",
            }
        )
    return jobs, f"sacct last {hours}h"


def combine_watch_jobs(
    live_jobs: list[dict],
    live_source: str,
    finished_jobs: list[dict],
    finished_source: str,
) -> tuple[list[dict], str]:
    jobs = [*live_jobs, *finished_jobs]
    source = live_source
    if finished_jobs:
        source += f" + {finished_source}"
    elif finished_source and finished_source != "sacct not found":
        source += f" + {finished_source}"
    return sort_watch_jobs(jobs), source


def load_live_watch_jobs(
    slurmctl_dir: str, *, timeout_seconds: float | None = None
) -> tuple[list[dict], str]:
    squeue = shutil.which("squeue")
    if squeue:
        command = [squeue, "--me", "--noheader", "--format=%i|%j|%T|%M|%D|%R"]
        try:
            proc = subprocess.run(
                command, text=True, capture_output=True, timeout=timeout_seconds
            )
        except subprocess.TimeoutExpired:
            timeout_text = (
                f"{timeout_seconds:g}s"
                if timeout_seconds is not None
                else "unknown timeout"
            )
            return [], f"squeue timed out after {timeout_text}"
        if proc.returncode == 0:
            jobs = []
            for line in proc.stdout.splitlines():
                parts = line.split("|", 5)
                if len(parts) != 6:
                    continue
                job_id, name, state, elapsed, nodes, reason = [
                    part.strip() for part in parts
                ]
                jobs.append(
                    {
                        "source": "squeue",
                        "job_id": job_id,
                        "job_name": name,
                        "state": state,
                        "elapsed": elapsed,
                        "nodes": nodes,
                        "reason": reason,
                    }
                )
            return jobs, "squeue --me"
        return [], proc.stderr.strip() or "squeue failed"

    fallback = []
    for record in load_submissions(slurmctl_dir):
        fallback.append(
            {
                "source": "captured",
                "job_id": record.get("job_id"),
                "job_name": record.get("normalized_options", {}).get("job_name")
                or record.get("script_path"),
                "state": "captured",
                "elapsed": "-",
                "nodes": "-",
                "reason": "squeue not found",
                "submission": record,
            }
        )
    return fallback, "captured submissions; squeue not found"


def load_watch_jobs(
    slurmctl_dir: str, *, timeout_seconds: float | None = None
) -> tuple[list[dict], str]:
    live_jobs, live_source = load_live_watch_jobs(
        slurmctl_dir, timeout_seconds=timeout_seconds
    )
    finished_jobs, finished_source = load_recent_finished_jobs(
        timeout_seconds, hours=24
    )
    return combine_watch_jobs(live_jobs, live_source, finished_jobs, finished_source)


def cancel_job(job_id: str, *, dry_run: bool) -> tuple[int, str]:
    return cancel_jobs([job_id], dry_run=dry_run)


def cancel_jobs(job_ids: list[str], *, dry_run: bool) -> tuple[int, str]:
    job_ids = [job_id for job_id in job_ids if job_id]
    if not job_ids:
        return 2, "no job ids to cancel"
    if dry_run:
        if len(job_ids) == 1:
            return 0, f"dry-run: would cancel job {job_ids[0]}"
        return 0, f"dry-run: would cancel jobs {', '.join(job_ids)}"
    scancel = shutil.which("scancel")
    if not scancel:
        return 127, "scancel not found"
    proc = subprocess.run([scancel, *job_ids], text=True, capture_output=True)
    output = (proc.stdout + proc.stderr).strip()
    return proc.returncode, output or f"scancel exited with {proc.returncode}"


def watch(args: argparse.Namespace) -> int:
    jobs, source = load_watch_jobs(args.slurmctl_dir)
    stats = watch_stats(jobs)
    print(f"source: {source}")
    print(
        f"stats: running {stats['running']} | "
        f"pending {stats['pending']} | "
        f"finished last 12h {stats['finished']}"
    )
    print("JOB_ID   STATE        ELAPSED    NODES  NAME                 REASON")
    for job in jobs:
        print(
            f"{job.get('job_id') or '-':<8} "
            f"{job.get('state') or '-':<12} "
            f"{job.get('elapsed') or '-':<10} "
            f"{job.get('nodes') or '-':<6} "
            f"{job.get('job_name') or '-':<20} "
            f"{job.get('reason') or '-'}"
        )
    return 0


class InteractiveShell:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.settings = load_settings()
        self.editing_setting: str | None = None
        self.confirm_cancel = False
        self.pending_slurmctl_dir: str | None = None
        self.screen: curses.window | None = None
        self.view = "home"
        self.query = ""
        self.search_active = False
        self.watch_visual_anchor: int | None = None
        self.pending_cancel_jobs: list[dict] = []
        self.reset_selection()
        self.message = ""
        self.dirty = True
        self.watch_cache: list[dict] = []
        self.watch_source = "not loaded"
        self.watch_error = ""
        self.watch_loaded_at = 0.0
        self.watch_refresh_interval_seconds = 2.0
        self.watch_finished_refresh_interval_seconds = 60.0
        self.watch_timeout_seconds = 3.0
        self.watch_refreshing = False
        self.watch_refresh_announce = False
        self.watch_next_token = 0
        self.watch_active_token = 0
        self.watch_results: queue.SimpleQueue[WatchRefreshResult] = queue.SimpleQueue()
        self.watch_finished_cache: list[dict] = []
        self.watch_finished_source = "sacct not loaded"
        self.watch_finished_loaded_at = 0.0
        self.submissions_cache: list[dict] = []
        self.scripts_cache: list[dict] = []
        self.viewer_path: Path | None = None
        self.viewer_lines: list[str] = []
        self.viewer_error = ""
        self.home_items = [
            {"name": "run", "detail": "Run a script with sbatch interception"},
            {"name": "show", "detail": "Browse captured submissions"},
            {"name": "watch", "detail": "Watch current Slurm jobs"},
            {"name": "settings", "detail": "Choose editor and metadata directory"},
        ]

    def run(self) -> int:
        try:
            return curses.wrapper(self._main)
        except KeyboardInterrupt:
            return 0

    def _main(self, screen: curses.window) -> int:
        self.screen = screen
        curses.curs_set(0)
        screen.keypad(True)
        screen.timeout(100)
        while True:
            self.tick()
            if self.dirty:
                self.draw()
                self.dirty = False
            key = screen.getch()
            if key == -1:
                continue
            if key == 3:
                return 0
            if self.handle_key(key):
                return 0
            self.dirty = True

    def tick(self) -> None:
        if self.drain_watch_results():
            self.dirty = True
        if (
            self.view == "watch"
            and not self.watch_visual_active()
            and not self.confirm_cancel
            and self.watch_refresh_due()
        ):
            if self.schedule_watch_refresh():
                self.dirty = True

    def filtered_home(self) -> list[dict]:
        return self.filter_items(
            self.home_items, lambda item: f"{item['name']} {item['detail']}"
        )

    def filtered_submissions(self) -> list[dict]:
        return self.filter_items(
            self.submissions_cache,
            lambda item: " ".join(
                str(item.get(key) or "")
                for key in (
                    "job_id",
                    "command_hash",
                    "script_path",
                    "resolved_stdout_path",
                    "resolved_stderr_path",
                )
            ),
        )

    def filtered_watch_jobs(self) -> list[dict]:
        return self.filter_items(
            self.watch_cache,
            lambda item: " ".join(
                str(item.get(key) or "")
                for key in (
                    "job_id",
                    "job_name",
                    "state",
                    "elapsed",
                    "nodes",
                    "reason",
                    "source",
                )
            ),
        )

    def filtered_scripts(self) -> list[dict]:
        return self.filter_items(self.scripts_cache, lambda item: item["path"])

    def refresh_submissions_cache(self) -> None:
        self.submissions_cache = load_submissions(self.args.slurmctl_dir)

    def refresh_scripts_cache(self) -> None:
        scripts = []
        for path in sorted(Path.cwd().rglob("*.sh")):
            if ".slurmctl" in path.parts:
                continue
            scripts.append({"path": str(path)})
        self.scripts_cache = scripts

    def filter_items(self, items: list[dict], text_fn) -> list[dict]:
        needle = self.query.lower().strip()
        if not needle:
            return items
        return [item for item in items if needle in text_fn(item).lower()]

    def watch_visual_active(self) -> bool:
        return self.view == "watch" and self.watch_visual_anchor is not None

    def watch_visual_bounds(self, count: int) -> tuple[int, int] | None:
        if self.watch_visual_anchor is None or count <= 0:
            return None
        anchor = max(0, min(self.watch_visual_anchor, count - 1))
        selected = max(0, min(self.selected, count - 1))
        return (min(anchor, selected), max(anchor, selected))

    def selected_watch_jobs(self) -> list[dict]:
        items = self.filtered_watch_jobs()
        if not items:
            return []
        bounds = self.watch_visual_bounds(len(items))
        if bounds is None:
            return [items[max(0, min(self.selected, len(items) - 1))]]
        start, end = bounds
        return items[start : end + 1]

    def toggle_watch_visual(self) -> None:
        items = self.filtered_watch_jobs()
        if not items:
            self.message = "No watch jobs to select."
            return
        if self.watch_visual_active():
            self.watch_visual_anchor = None
            self.message = "Visual selection cleared."
            return
        self.clamp_selected(len(items))
        self.watch_visual_anchor = self.selected
        self.message = ""

    def clear_watch_visual(self) -> None:
        self.watch_visual_anchor = None

    def watch_refresh_due(self) -> bool:
        if self.watch_refreshing:
            return False
        if self.watch_loaded_at == 0.0:
            return True
        now = time.monotonic()
        return now - self.watch_loaded_at >= self.watch_refresh_interval_seconds

    def watch_finished_refresh_due(self) -> bool:
        if self.watch_finished_loaded_at == 0.0:
            return True
        now = time.monotonic()
        return (
            now - self.watch_finished_loaded_at
            >= self.watch_finished_refresh_interval_seconds
        )

    def schedule_watch_refresh(self, *, announce: bool = False) -> bool:
        if self.watch_refreshing:
            if announce:
                self.message = "Watch refresh already in progress."
            return False

        self.watch_next_token += 1
        token = self.watch_next_token
        refresh_finished = announce or self.watch_finished_refresh_due()
        self.watch_active_token = token
        self.watch_refreshing = True
        self.watch_refresh_announce = announce
        self.watch_error = ""
        if announce:
            self.message = "Refreshing watch..."

        thread = threading.Thread(
            target=self.load_watch_jobs_in_background,
            args=(
                token,
                refresh_finished,
                list(self.watch_finished_cache),
                self.watch_finished_source,
                self.watch_finished_loaded_at,
            ),
            daemon=True,
        )
        thread.start()
        return True

    def load_watch_jobs_in_background(
        self,
        token: int,
        refresh_finished: bool,
        cached_finished: list[dict],
        cached_finished_source: str,
        cached_finished_loaded_at: float,
    ) -> None:
        try:
            finished_jobs = cached_finished
            finished_source = cached_finished_source
            finished_loaded_at = cached_finished_loaded_at
            if refresh_finished:
                finished_jobs, finished_source = load_recent_finished_jobs(
                    self.watch_timeout_seconds, hours=24
                )
                finished_loaded_at = time.monotonic()
            live_jobs, live_source = load_live_watch_jobs(
                self.args.slurmctl_dir, timeout_seconds=self.watch_timeout_seconds
            )
            jobs, source = combine_watch_jobs(
                live_jobs, live_source, finished_jobs, finished_source
            )
            error = ""
            if not source.startswith("squeue --me") and not source.startswith(
                "captured submissions"
            ):
                error = source
            result = WatchRefreshResult(
                token,
                jobs,
                source,
                time.monotonic(),
                error,
                finished_jobs if refresh_finished else None,
                finished_source if refresh_finished else "",
                finished_loaded_at if refresh_finished else 0.0,
            )
        except (
            Exception
        ) as exc:  # Keep the UI alive even if an external tool fails strangely.
            result = WatchRefreshResult(
                token, [], "watch refresh failed", time.monotonic(), str(exc)
            )
        self.watch_results.put(result)

    def drain_watch_results(self) -> bool:
        changed = False
        while True:
            try:
                result = self.watch_results.get_nowait()
            except queue.Empty:
                break
            if result.token != self.watch_active_token:
                continue

            self.watch_refreshing = False
            self.watch_loaded_at = result.loaded_at
            self.watch_error = result.error
            if result.finished_jobs is not None:
                self.watch_finished_cache = result.finished_jobs
                self.watch_finished_source = result.finished_source
                self.watch_finished_loaded_at = result.finished_loaded_at
            if result.error:
                if not self.watch_cache:
                    self.watch_cache = result.jobs
                    self.watch_source = result.source
                if self.watch_refresh_announce:
                    self.message = f"Watch refresh failed: {result.error}"
            else:
                self.watch_cache = result.jobs
                self.watch_source = result.source
                if self.watch_refresh_announce:
                    self.message = f"Refreshed watch from {result.source}"
            self.watch_refresh_announce = False
            changed = True
        return changed

    def clamp_selected(self, count: int) -> None:
        if count <= 0:
            self.reset_selection()
        else:
            self.selected = max(0, min(self.selected, count - 1))

    def visible_slice(self, items: list[dict], max_rows: int) -> tuple[int, list[dict]]:
        max_rows = max(0, max_rows)
        if max_rows <= 0 or not items:
            self.scroll_top = 0
            return 0, []
        if self.selected < self.scroll_top:
            self.scroll_top = self.selected
        if self.selected >= self.scroll_top + max_rows:
            self.scroll_top = self.selected - max_rows + 1
        max_top = max(0, len(items) - max_rows)
        self.scroll_top = max(0, min(self.scroll_top, max_top))
        return self.scroll_top, items[self.scroll_top : self.scroll_top + max_rows]

    def content_rows(self, height: int, first_row: int) -> int:
        return max(0, height - first_row - 2)

    def reset_selection(self) -> None:
        self.selected = 0
        self.scroll_top = 0
        self.clear_watch_visual()

    def prepare_view(self, view: str) -> None:
        if view == "run":
            self.refresh_scripts_cache()
        elif view == "show":
            self.refresh_submissions_cache()
        elif view == "watch":
            self.schedule_watch_refresh()

    def draw(self) -> None:
        assert self.screen is not None
        self.screen.erase()
        height, width = self.screen.getmaxyx()
        self.draw_header(height, width)

        if self.view == "home":
            self.draw_home(height, width)
        elif self.view == "run":
            self.draw_run(height, width)
        elif self.view == "show":
            self.draw_show(height, width)
        elif self.view == "watch":
            self.draw_watch(height, width)
        elif self.view == "actions":
            self.draw_actions(height, width)
        elif self.view == "watch_actions":
            self.draw_watch_actions(height, width)
        elif self.view == "viewer":
            self.draw_viewer(height, width)
        elif self.view == "settings":
            self.draw_settings(height, width)

        if height > 2:
            status = self.status_line()
            self.add(height - 2, 0, status[: width - 1])
        prompt = {
            "home": "Search commands",
            "run": "Search/type script path",
            "show": "Search submissions",
            "watch": "Search jobs",
            "actions": "Search actions",
            "watch_actions": "Search actions",
            "viewer": "Search text",
            "settings": "Search/edit settings",
        }.get(self.view, "Search")
        marker = "*" if self.search_active else " "
        self.add(
            height - 1,
            0,
            f"{marker} {prompt}: {self.query}"[: width - 1],
            curses.A_REVERSE,
        )
        self.screen.refresh()

    def status_line(self) -> str:
        if self.confirm_cancel:
            return self.message or "Type CANCEL and press Enter to confirm."
        if self.watch_visual_active():
            if self.message:
                return self.message
            count = len(self.selected_watch_jobs())
            return f"Visual: {count} selected | up/down extend | C cancel | v clear | B back"
        return (
            self.message
            or "Enter select | arrows move | Tab search on/off | B back | q quit"
        )

    def draw_header(self, height: int, width: int) -> None:
        if height < 2:
            return
        view_title = {
            "home": "Home",
            "run": "Run",
            "show": "Submissions",
            "watch": "Watch",
            "actions": "Actions",
            "watch_actions": "Job Actions",
            "viewer": "Viewer",
            "settings": "Settings",
        }.get(self.view, self.view.title())
        if self.view == "show":
            context = f"{len(self.filtered_submissions())} shown / {len(self.submissions_cache)} total"
        elif self.view == "watch":
            context = f"{len(self.filtered_watch_jobs())} shown / {len(self.watch_cache)} total | {self.watch_source}"
            if self.watch_refreshing:
                context += " | refreshing"
            if self.watch_error:
                context += f" | {self.watch_error}"
        elif self.view == "run":
            context = f"{len(self.filtered_scripts())} scripts"
        elif self.view == "actions":
            record = getattr(self, "active_record", None) or {}
            context = f"job {record.get('job_id') or '-'} | {record.get('script_path') or '-'}"
        elif self.view == "watch_actions":
            job = getattr(self, "active_job", None) or {}
            context = f"job {job.get('job_id') or '-'} | {job.get('state') or '-'}"
        elif self.view == "settings":
            context = str(config_path())
        else:
            context = str(Path.cwd())
        line = f" slurmctl  >  {view_title}  |  {context}"
        self.add(0, 0, line.ljust(width - 1)[: width - 1], curses.A_REVERSE)
        self.add(1, 0, ("-" * max(0, width - 1))[: width - 1])

    def draw_home(self, height: int, width: int) -> None:
        items = self.filtered_home()
        self.clamp_selected(len(items))
        start, visible = self.visible_slice(items, self.content_rows(height, 2))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            self.add(
                idx + 2, 2, f"{item['name']:<8} {item['detail']}"[: width - 4], attr
            )

    def draw_run(self, height: int, width: int) -> None:
        items = self.filtered_scripts()
        self.clamp_selected(len(items))
        self.add(
            2,
            2,
            "Enter runs the selected script, or the typed path if there is no match."[
                : width - 4
            ],
        )
        start, visible = self.visible_slice(items, self.content_rows(height, 4))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            self.add(idx + 4, 2, item["path"][: width - 4], attr)

    def draw_show(self, height: int, width: int) -> None:
        items = self.filtered_submissions()
        self.clamp_selected(len(items))
        header = f"{'JOB_ID':<8} {'HASH':<16} {'SCRIPT':<22} {'OUT':<24} ERR"
        self.add(2, 0, header[: width - 1], curses.A_BOLD)
        start, visible = self.visible_slice(items, self.content_rows(height, 3))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            row = (
                f"{item.get('job_id') or '-':<8} "
                f"{item.get('command_hash') or '-':<16} "
                f"{item.get('script_path') or '-':<22} "
                f"{item.get('resolved_stdout_path') or '-':<24} "
                f"{item.get('resolved_stderr_path') or '-'}"
            )
            self.add(idx + 3, 0, row[: width - 1], attr)

    def draw_watch(self, height: int, width: int) -> None:
        items = self.filtered_watch_jobs()
        self.clamp_selected(len(items))
        bounds = self.watch_visual_bounds(len(items))
        stats = watch_stats(self.watch_cache)
        stats_line = (
            f"running {stats['running']} | "
            f"pending {stats['pending']} | "
            f"finished last 12h {stats['finished']}"
        )
        self.add(2, 0, stats_line[: width - 1], curses.A_BOLD)
        header = f"{'JOB_ID':<8} {'STATE':<12} {'ELAPSED':<10} {'NODES':<6} {'NAME':<22} REASON"
        self.add(3, 0, header[: width - 1], curses.A_BOLD)
        max_rows = self.content_rows(height, 4)
        start, visible = self.visible_slice(items, max(0, max_rows - 3))
        row_y = 4
        previous_group = (
            job_group(items[start - 1]) if start > 0 and start <= len(items) else None
        )
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            current_group = job_group(item)
            if (
                previous_group
                and current_group != previous_group
                and row_y <= height - 3
            ):
                label = f" {current_group.upper()} "
                divider = label.center(max(0, width - 1), "-")
                self.add(row_y, 0, divider[: width - 1], curses.A_DIM)
                row_y += 1
            if row_y > height - 3:
                break
            in_visual_range = (
                bounds is not None and bounds[0] <= absolute_idx <= bounds[1]
            )
            attr = (
                curses.A_REVERSE
                if absolute_idx == self.selected or in_visual_range
                else curses.A_NORMAL
            )
            if absolute_idx == self.selected:
                attr |= curses.A_BOLD
            row = (
                f"{item.get('job_id') or '-':<8} "
                f"{item.get('state') or '-':<12} "
                f"{item.get('elapsed') or '-':<10} "
                f"{item.get('nodes') or '-':<6} "
                f"{item.get('job_name') or '-':<22} "
                f"{item.get('reason') or '-'}"
            )
            self.add(row_y, 0, row[: width - 1], attr)
            row_y += 1
            previous_group = current_group

    def action_items(self) -> list[dict]:
        actions = [
            {"key": "D", "name": "delete", "detail": "Delete this captured run"},
            {
                "key": "R",
                "name": "rerun",
                "detail": "Submit the same sbatch recipe again",
            },
            {
                "key": "OO",
                "name": "open output file",
                "detail": "View resolved stdout file",
            },
            {
                "key": "OE",
                "name": "open error file",
                "detail": "View resolved stderr file",
            },
            {
                "key": "JSON",
                "name": "open run json",
                "detail": "View captured metadata",
            },
            {"key": "B", "name": "back", "detail": "Return to show"},
        ]
        return self.filter_items(
            actions, lambda item: f"{item['key']} {item['name']} {item['detail']}"
        )

    def watch_action_items(self) -> list[dict]:
        actions = [
            {
                "key": "C",
                "name": "cancel job",
                "detail": "Prompt, then run scancel for this job",
            },
            {
                "key": "R",
                "name": "rerun",
                "detail": "Rerun captured sbatch recipe when available",
            },
            {
                "key": "OO",
                "name": "open output file",
                "detail": "Open captured stdout file when available",
            },
            {
                "key": "OE",
                "name": "open error file",
                "detail": "Open captured stderr file when available",
            },
            {
                "key": "JSON",
                "name": "open run json",
                "detail": "Open captured metadata when available",
            },
            {"key": "B", "name": "back", "detail": "Return to watch"},
        ]
        return self.filter_items(
            actions, lambda item: f"{item['key']} {item['name']} {item['detail']}"
        )

    def settings_items(self) -> list[dict]:
        items = [
            {
                "key": "editor",
                "name": "default editor",
                "value": self.settings.get("editor")
                or "(VISUAL/EDITOR/editor/nano/vi)",
                "detail": "Command used for OO, OE, and JSON",
            },
            {
                "key": "slurmctl_dir",
                "name": ".slurmctl dir",
                "value": self.settings.get("slurmctl_dir") or ".slurmctl",
                "detail": "Default metadata directory",
            },
        ]
        if self.editing_setting:
            return items
        return self.filter_items(
            items, lambda item: f"{item['name']} {item['value']} {item['detail']}"
        )

    def draw_settings(self, height: int, width: int) -> None:
        items = self.settings_items()
        self.clamp_selected(len(items))
        self.add(
            2,
            2,
            "Enter edits the selected setting. Empty editor means environment/default fallback."[
                : width - 4
            ],
        )
        start, visible = self.visible_slice(items, self.content_rows(height, 4))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            row = f"{item['name']:<18} {item['value']:<32} {item['detail']}"
            self.add(idx + 4, 2, row[: width - 4], attr)

    def draw_actions(self, height: int, width: int) -> None:
        record = getattr(self, "active_record", None) or {}
        self.add(
            2,
            2,
            f"Selected: {record.get('job_id') or '-'} {record.get('script_path') or '-'}"[
                : width - 4
            ],
        )
        items = self.action_items()
        self.clamp_selected(len(items))
        start, visible = self.visible_slice(items, self.content_rows(height, 4))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            self.add(
                idx + 4,
                2,
                f"{item['key']:<5} {item['name']:<18} {item['detail']}"[: width - 4],
                attr,
            )

    def draw_watch_actions(self, height: int, width: int) -> None:
        job = getattr(self, "active_job", None) or {}
        self.add(
            2,
            2,
            f"Selected: {job.get('job_id') or '-'} {job.get('state') or '-'} {job.get('job_name') or '-'}"[
                : width - 4
            ],
        )
        if self.confirm_cancel:
            self.add(
                3,
                2,
                "Type CANCEL in the bottom bar and press Enter to cancel this job."[
                    : width - 4
                ],
                curses.A_BOLD,
            )
        items = self.watch_action_items()
        self.clamp_selected(len(items))
        start, visible = self.visible_slice(items, self.content_rows(height, 5))
        for idx, item in enumerate(visible):
            absolute_idx = start + idx
            attr = (
                curses.A_REVERSE if absolute_idx == self.selected else curses.A_NORMAL
            )
            self.add(
                idx + 5,
                2,
                f"{item['key']:<5} {item['name']:<18} {item['detail']}"[: width - 4],
                attr,
            )

    def draw_viewer(self, height: int, width: int) -> None:
        path = self.viewer_path
        self.add(2, 2, str(path or "")[: width - 4], curses.A_BOLD)
        if path is None:
            self.add(4, 2, "No file selected."[: width - 4])
            return
        if self.viewer_error:
            self.add(4, 2, self.viewer_error[: width - 4])
            return
        lines = self.viewer_lines
        if self.query:
            lines = [line for line in lines if self.query.lower() in line.lower()]
        for idx, line in enumerate(lines[: self.content_rows(height, 4)]):
            self.add(idx + 4, 0, line[: width - 1])

    def load_viewer(self, path: Path) -> None:
        self.viewer_path = path
        self.viewer_lines = []
        self.viewer_error = ""
        if not path.exists():
            self.viewer_error = "File does not exist."
            return
        try:
            self.viewer_lines = path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        except OSError as exc:
            self.viewer_error = str(exc)

    def add(self, y: int, x: int, text: str, attr: int = curses.A_NORMAL) -> None:
        assert self.screen is not None
        height, width = self.screen.getmaxyx()
        if y >= height or x >= width:
            return
        try:
            self.screen.addstr(y, x, text[: max(0, width - x - 1)], attr)
        except curses.error:
            pass

    def handle_key(self, key: int) -> bool:
        if key == curses.KEY_UP or (key == ord("k") and not self.search_active):
            self.selected = max(0, self.selected - 1)
            return False
        if key == curses.KEY_DOWN or (key == ord("j") and not self.search_active):
            self.selected += 1
            return False
        if key == ord("\t"):
            self.search_active = not self.search_active
            return False
        if key in (ord("\n"), curses.KEY_ENTER, 10, 13):
            self.activate()
            return False
        if self.search_active:
            if key in (curses.KEY_BACKSPACE, 127, 8, curses.KEY_DC, curses.KEY_LEFT):
                self.query = self.query[:-1]
                self.reset_selection()
                return False
            if key == curses.KEY_RESIZE:
                return False
            if 32 <= key <= 126:
                self.query += chr(key)
                self.reset_selection()
            return False
        if key == 27 and self.watch_visual_active():
            self.clear_watch_visual()
            self.message = "Visual selection cleared."
            return False
        if key in (ord("v"), ord("V")) and self.view == "watch":
            self.toggle_watch_visual()
            return False
        if (
            key in (ord("c"), ord("C"))
            and self.view == "watch"
            and self.watch_visual_active()
        ):
            self.begin_cancel_jobs(self.selected_watch_jobs())
            return False
        if key == ord("q") and not self.search_active:
            return True
        if key in (ord("r"), ord("R")) and self.view == "watch":
            self.schedule_watch_refresh(announce=True)
            return False
        if key in (ord("b"), ord("B")):
            self.go_back()
            return False
        if key in (curses.KEY_BACKSPACE, 127, 8):
            self.go_back()
            return False
        if key in (curses.KEY_DC, curses.KEY_LEFT):
            self.go_back()
            return False
        if key == curses.KEY_RESIZE:
            return False
        if 32 <= key <= 126:
            char = chr(key)
            if self.view == "actions":
                self.maybe_activate_shortcut(char)
            elif self.view == "watch_actions":
                self.maybe_activate_watch_shortcut(char)
            else:
                self.message = "Press Tab to search."
        return False

    def go_back(self) -> None:
        if self.view == "home":
            return
        if self.view in {"run", "show", "watch"}:
            self.view = "home"
        elif self.view == "actions":
            self.view = "show"
        elif self.view == "watch_actions":
            self.view = "watch"
            self.confirm_cancel = False
        elif self.view == "viewer":
            self.view = "actions"
        elif self.view == "settings":
            self.view = "home"
            self.editing_setting = None
            self.pending_slurmctl_dir = None
        self.query = ""
        self.search_active = False
        self.reset_selection()
        self.message = ""

    def activate(self) -> None:
        if self.confirm_cancel:
            self.confirm_cancel_action()
            return
        if self.view == "settings" and self.editing_setting:
            self.save_active_setting()
            return
        if self.view == "settings" and self.pending_slurmctl_dir is not None:
            self.finish_slurmctl_dir_change()
            return
        if self.view == "home":
            items = self.filtered_home()
            if not items:
                return
            self.view = items[self.selected]["name"]
            self.prepare_view(self.view)
            self.query = ""
            self.search_active = False
            self.reset_selection()
            self.message = ""
            return
        if self.view == "run":
            self.activate_run()
            return
        if self.view == "show":
            items = self.filtered_submissions()
            if not items:
                return
            self.active_record = items[self.selected]
            self.view = "actions"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            return
        if self.view == "watch":
            if self.watch_visual_active():
                self.message = (
                    "Press C to cancel the visual selection, or v to clear it."
                )
                return
            items = self.filtered_watch_jobs()
            if not items:
                return
            self.active_job = items[self.selected]
            self.confirm_cancel = False
            self.view = "watch_actions"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            return
        if self.view == "actions":
            items = self.action_items()
            if items:
                self.perform_action(items[self.selected]["key"])
            return
        if self.view == "watch_actions":
            items = self.watch_action_items()
            if items:
                self.perform_watch_action(items[self.selected]["key"])
            return
        if self.view == "viewer":
            self.view = "actions"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            return
        if self.view == "settings":
            items = self.settings_items()
            if not items:
                return
            item = items[self.selected]
            self.editing_setting = item["key"]
            raw_value = self.settings.get(item["key"], "")
            self.query = raw_value
            self.search_active = True
            self.message = f"Editing {item['name']}; Enter saves."

    def activate_run(self) -> None:
        items = self.filtered_scripts()
        script = items[self.selected]["path"] if items else self.query.strip()
        if not script:
            self.message = "No script selected."
            return
        self.suspend_and_run(
            ["run", script], rerender_message=f"Finished running {script}"
        )

    def maybe_activate_shortcut(self, char: str) -> None:
        self.query += char
        upper = self.query.upper()
        keys = {item["key"] for item in self.action_items()}
        if upper in keys:
            self.perform_action(upper)

    def maybe_activate_watch_shortcut(self, char: str) -> None:
        self.query += char
        upper = self.query.upper()
        keys = {item["key"] for item in self.watch_action_items()}
        if upper in keys:
            self.perform_watch_action(upper)

    def perform_action(self, key: str) -> None:
        record = getattr(self, "active_record", None)
        if not record:
            return
        if key == "B":
            self.view = "show"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            return
        if key == "D":
            delete_submission(record, self.args.slurmctl_dir)
            self.refresh_submissions_cache()
            self.view = "show"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            self.message = f"Deleted run {record.get('run_id') or ''}".strip()
            return
        if key == "R":
            self.suspend_and_rerun(record)
            return
        if key == "OO":
            self.open_record_path_in_editor(record, "resolved_stdout_path")
            return
        if key == "OE":
            self.open_record_path_in_editor(record, "resolved_stderr_path")
            return
        if key == "JSON":
            run_id = record.get("run_id")
            if run_id:
                self.suspend_and_edit(
                    Path(self.args.slurmctl_dir) / "runs" / f"{run_id}.json"
                )

    def perform_watch_action(self, key: str) -> None:
        job = getattr(self, "active_job", None)
        if not job:
            return
        if key == "B":
            self.view = "watch"
            self.query = ""
            self.search_active = False
            self.reset_selection()
            self.confirm_cancel = False
            return
        submission = job.get("submission") or self.find_submission_for_job(
            job.get("job_id")
        )
        if key == "C":
            self.begin_cancel_jobs([job])
            return
        if key == "R":
            if submission:
                self.suspend_and_rerun(submission, return_view="watch")
            else:
                self.message = "No captured sbatch recipe for this job."
            return
        if key == "OO":
            if submission:
                self.open_record_path_in_editor(submission, "resolved_stdout_path")
            else:
                self.message = "No captured stdout path for this job."
            return
        if key == "OE":
            if submission:
                self.open_record_path_in_editor(submission, "resolved_stderr_path")
            else:
                self.message = "No captured stderr path for this job."
            return
        if key == "JSON":
            if submission and submission.get("run_id"):
                self.suspend_and_edit(
                    Path(self.args.slurmctl_dir)
                    / "runs"
                    / f"{submission['run_id']}.json"
                )
            else:
                self.message = "No captured metadata for this job."

    def begin_cancel_jobs(self, jobs: list[dict]) -> None:
        if not jobs:
            self.message = "No jobs selected."
            self.query = ""
            return
        cancelable = [
            job
            for job in jobs
            if job.get("source") == "squeue" and str(job.get("job_id") or "")
        ]
        if len(cancelable) != len(jobs):
            self.message = "Cancel is only available for live squeue jobs."
            self.query = ""
            return
        self.pending_cancel_jobs = cancelable
        self.confirm_cancel = True
        self.query = ""
        self.search_active = True
        job_ids = [str(job.get("job_id") or "") for job in cancelable]
        if len(job_ids) == 1:
            self.message = (
                f"Confirm cancel for job {job_ids[0]}: type CANCEL and press Enter."
            )
        else:
            preview = ", ".join(job_ids[:3])
            suffix = "" if len(job_ids) <= 3 else f", +{len(job_ids) - 3} more"
            self.message = f"Confirm cancel for {len(job_ids)} jobs ({preview}{suffix}): type CANCEL and press Enter."

    def confirm_cancel_action(self) -> None:
        jobs = self.pending_cancel_jobs
        job_ids = [
            str(job.get("job_id") or "") for job in jobs if str(job.get("job_id") or "")
        ]
        if self.query.strip() != "CANCEL":
            self.message = "Cancel aborted; confirmation text did not match CANCEL."
            self.confirm_cancel = False
            self.pending_cancel_jobs = []
            self.query = ""
            self.search_active = False
            return
        code, output = cancel_jobs(job_ids, dry_run=self.args.dry_run)
        target = (
            f"{len(job_ids)} jobs" if len(job_ids) != 1 else f"job {job_ids[0] or '-'}"
        )
        self.message = f"Cancel {target} exited {code}: {output}"
        self.confirm_cancel = False
        self.pending_cancel_jobs = []
        self.query = ""
        self.search_active = False
        self.view = "watch"
        self.reset_selection()
        self.schedule_watch_refresh()

    def find_submission_for_job(self, job_id: object) -> dict | None:
        if not job_id:
            return None
        job_id_text = str(job_id)
        if not self.submissions_cache:
            self.refresh_submissions_cache()
        for record in reversed(self.submissions_cache):
            if str(record.get("job_id") or "") == job_id_text:
                return record
        return None

    def open_record_path_in_editor(self, record: dict, key: str) -> None:
        value = self.resolve_record_path_value(record, key)
        if not value:
            self.message = "No path recorded."
            return
        base = Path(record.get("submit_cwd") or ".")
        path = Path(value)
        self.suspend_and_edit(path if path.is_absolute() else base / path)

    def resolve_record_path_value(self, record: dict, key: str) -> str | None:
        value = record.get(key)
        template_key = {
            "resolved_stdout_path": "stdout_template",
            "resolved_stderr_path": "stderr_template",
        }.get(key)
        if template_key and (not value or "%" in str(value)):
            template = record.get(template_key) or value
            options = record.get("normalized_options", {})
            value = resolve_output_template(
                template,
                record.get("job_id"),
                options.get("job_name"),
                record.get("script_path"),
            )
        elif value and "%" in str(value):
            options = record.get("normalized_options", {})
            value = resolve_output_template(
                value,
                record.get("job_id"),
                options.get("job_name"),
                record.get("script_path"),
            )
        return str(value) if value else None

    def suspend_and_edit(self, path: Path) -> None:
        assert self.screen is not None
        resolved = path.resolve()
        if not resolved.exists() and not resolved.parent.exists():
            self.message = f"Cannot open {resolved}: parent directory does not exist."
            return
        curses.def_prog_mode()
        curses.endwin()
        try:
            command = [*editor_command(self.settings.get("editor")), str(resolved)]
            code = subprocess.run(command).returncode
            self.message = f"Editor exited with {code}: {resolved}"
            self.query = ""
            self.search_active = False
            self.reset_selection()
        finally:
            curses.reset_prog_mode()
            self.screen.keypad(True)
            self.screen.timeout(100)

    def save_active_setting(self) -> None:
        key = self.editing_setting
        if not key:
            return
        value = self.query.strip()
        if key == "slurmctl_dir" and not value:
            value = ".slurmctl"
        if key == "slurmctl_dir":
            old_value = self.args.slurmctl_dir
            old_dir = resolve_from_cwd(old_value)
            new_dir = resolve_from_cwd(value)
            if old_dir != new_dir and has_slurmctl_data(old_dir):
                self.pending_slurmctl_dir = value
                self.editing_setting = None
                self.query = ""
                self.search_active = True
                self.message = f"Move data from {old_dir} to {new_dir}? Type MOVE or SKIP, then Enter."
                return
        self.settings[key] = value
        save_settings(self.settings)
        if key == "slurmctl_dir":
            self.args.slurmctl_dir = value
            self.refresh_submissions_cache()
            self.schedule_watch_refresh()
        self.editing_setting = None
        self.query = ""
        self.search_active = False
        self.reset_selection()
        self.message = f"Saved {key} in {config_path()}"

    def finish_slurmctl_dir_change(self) -> None:
        value = self.pending_slurmctl_dir
        if value is None:
            return
        answer = self.query.strip().upper()
        if answer not in {"MOVE", "SKIP"}:
            self.message = (
                "Type MOVE to move current data, or SKIP to only change the setting."
            )
            return
        old_dir = resolve_from_cwd(self.args.slurmctl_dir)
        new_dir = resolve_from_cwd(value)
        if answer == "MOVE":
            try:
                move_slurmctl_data(old_dir, new_dir)
            except OSError as exc:
                self.message = f"Move failed: {exc}"
                self.query = ""
                return
        self.settings["slurmctl_dir"] = value
        save_settings(self.settings)
        self.args.slurmctl_dir = value
        self.refresh_submissions_cache()
        self.schedule_watch_refresh()
        self.pending_slurmctl_dir = None
        self.query = ""
        self.search_active = False
        self.reset_selection()
        moved = "moved data and " if answer == "MOVE" else ""
        self.message = f"{moved}saved slurmctl_dir in {config_path()}"

    def suspend_and_run(
        self, command_argv: list[str], *, rerender_message: str
    ) -> None:
        assert self.screen is not None
        curses.def_prog_mode()
        curses.endwin()
        try:
            namespace = argparse.Namespace(
                command="run",
                script=command_argv[1],
                script_args=[],
                dry_run=self.args.dry_run,
                verbose=self.args.verbose,
                slurmctl_dir=self.args.slurmctl_dir,
            )
            code = run_script(namespace)
            input(f"\nslurmctl exited with {code}. Press Enter to return.")
            self.refresh_submissions_cache()
            self.message = rerender_message
        finally:
            curses.reset_prog_mode()
            self.screen.keypad(True)
            self.screen.timeout(100)

    def suspend_and_rerun(self, record: dict, return_view: str = "show") -> None:
        assert self.screen is not None
        curses.def_prog_mode()
        curses.endwin()
        try:
            code = run_sbatch_args(
                list(record.get("raw_args") or []),
                submit_cwd=record.get("submit_cwd"),
                slurmctl_dir=self.args.slurmctl_dir,
                dry_run=self.args.dry_run,
                verbose=self.args.verbose,
            )
            input(f"\nrerun exited with {code}. Press Enter to return.")
            self.refresh_submissions_cache()
            if return_view == "watch":
                self.schedule_watch_refresh()
            self.view = return_view
            self.query = ""
            self.search_active = False
            self.reset_selection()
            self.message = f"Reran {record.get('script_path') or 'submission'}"
        finally:
            curses.reset_prog_mode()
            self.screen.keypad(True)
            self.screen.timeout(100)


def doctor(args: argparse.Namespace) -> int:
    real_sbatch = discover_real_sbatch()
    settings = load_settings()
    print(f"PATH: {os.environ.get('PATH', '')}")
    print(f"python: {sys.executable}")
    print(f"sbatch: {real_sbatch or 'not found'}")
    print(f"squeue: {shutil.which('squeue') or 'not found'}")
    print(f"sacct: {shutil.which('sacct') or 'not found'}")
    print(f"scancel: {shutil.which('scancel') or 'not found'}")
    print(f"settings: {config_path()}")
    print(f"editor: {settings.get('editor') or '(VISUAL/EDITOR/editor/nano/vi)'}")
    print(f"slurmctl dir: {Path(args.slurmctl_dir).resolve()}")
    if not real_sbatch:
        print("Use --dry-run with run to simulate sbatch on systems without Slurm.")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()

    # Allow slurmctl options after `run SCRIPT`, e.g.:
    #   python slurmctl.py run examples/basic/runner.sh --dry-run
    sbatch_index = find_sbatch_command(argv)
    if sbatch_index is not None:
        tool_opts, normalized_prefix = extract_tool_options(argv[:sbatch_index])
        if tool_opts.help:
            parser.print_help()
            return 0
        if normalized_prefix:
            parser.parse_args([*normalized_prefix, "sbatch"])
        settings = load_settings()
        args = argparse.Namespace(
            command="sbatch",
            sbatch_args=argv[sbatch_index + 1 :],
            dry_run=tool_opts.dry_run,
            verbose=tool_opts.verbose,
            slurmctl_dir=tool_opts.slurmctl_dir or settings["slurmctl_dir"],
        )
        return sbatch(args)

    tool_opts, normalized_argv = extract_tool_options(argv)
    if tool_opts.help:
        parser.print_help()
        return 0
    args = parser.parse_args(normalized_argv)
    settings = load_settings()
    args.dry_run = bool(getattr(args, "dry_run", False) or tool_opts.dry_run)
    args.verbose = bool(getattr(args, "verbose", False) or tool_opts.verbose)
    args.slurmctl_dir = (
        tool_opts.slurmctl_dir
        or getattr(args, "slurmctl_dir", None)
        or settings["slurmctl_dir"]
    )

    if args.command == "run":
        return run_script(args)
    if args.command == "sbatch":
        return sbatch(args)
    if args.command == "show":
        return show(args)
    if args.command == "watch":
        return watch(args)
    if args.command == "doctor":
        return doctor(args)
    if args.command in {None, "interactive", "shell", "ui"}:
        return InteractiveShell(args).run()
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
