# slurmctl

`slurmctl` is a minimal, dependency-free Python CLI for intercepting `sbatch`
calls made by your runner scripts.

It works by putting a temporary fake `sbatch` at the front of `PATH`. The fake
`sbatch` parses the submitted command and script directives, calls the real
`sbatch` when available, captures the Slurm job id, and writes metadata to a
`.slurmctl` directory.

## Requirements

- Python 3.11+
- Standard library only
- Slurm tools for real submissions: `sbatch`, `squeue`, `scancel`
- Optional Slurm accounting for recently finished jobs: `sacct`

`--dry-run` works without Slurm installed.

## Install

Single-file install:

```bash
mkdir -p ~/.local/bin
curl -fsSL https://raw.githubusercontent.com/pietrosig/slurmctl/main/slurmctl.py -o ~/.local/bin/slurmctl
chmod +x ~/.local/bin/slurmctl
```

The installed `slurmctl` launcher looks for `python3.13`, `python3.12`,
`python3.11`, then `python3`, and only uses an interpreter if it is Python
3.11+. You can force a specific interpreter with `SLURMCTL_PYTHON`.

Make sure `~/.local/bin` is on your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

For local development from this repo:

```bash
chmod +x slurmctl.py
./slurmctl.py doctor
```

## Quick Start

Check your environment:

```bash
slurmctl doctor
```

Run a script and intercept its `sbatch` calls:

```bash
slurmctl run examples/basic/runner.sh
```

Try it without Slurm:

```bash
slurmctl run examples/basic/runner.sh --dry-run
```

Show captured submissions:

```bash
slurmctl show
```

Watch current jobs:

```bash
slurmctl watch
```

Open the interactive shell:

```bash
slurmctl
```

Equivalent:

```bash
slurmctl interactive
slurmctl shell
slurmctl ui
```

## Commands

### `run`

```bash
slurmctl run SCRIPT [SCRIPT_ARGS...] [--dry-run]
```

Runs `SCRIPT` while intercepting every `sbatch` command it invokes.

### `show`

```bash
slurmctl show
```

Prints captured submissions with job id, command hash, script path, stdout path,
and stderr path.

### `watch`

```bash
slurmctl watch
```

Uses `squeue --me` when available. If `sacct` is available, watch also includes
jobs that finished in the last 12 hours. If `squeue` is not installed, it falls
back to captured submissions so the view still works off-cluster.

### `doctor`

```bash
slurmctl doctor
```

Prints detected Slurm tools, settings path, editor, and metadata directory.

## Interactive Shell

Start with:

```bash
slurmctl
```

Home views:

- `run`: select or type a shell script to run
- `show`: browse captured submissions
- `watch`: browse live Slurm jobs or captured fallback jobs
- `settings`: edit default editor and metadata directory

Controls:

- Arrow up/down: move selection
- `j`/`k`: move selection when not in search mode
- `Tab`: toggle search/edit mode in the bottom bar
- Enter: activate selected row or save current setting
- `B`: go back when not in search mode
- `q`: quit when not in search mode

In `show`, selecting a submission opens actions:

- `D`: delete captured run metadata
- `R`: rerun the captured `sbatch` recipe
- `OO`: open the resolved stdout file in your editor
- `OE`: open the resolved stderr file in your editor
- `JSON`: open captured run metadata
- `B`: back

In `watch`, selecting a job opens similar actions plus:

- `C`: cancel a live Slurm job

The interactive `watch` view caches Slurm results briefly so large job lists do
not make every redraw slow. Press `R` in the watch list to refresh manually.

Cancel is guarded: you must type `CANCEL` and press Enter before `scancel` is
called. With `--dry-run`, cancel only reports what would happen.

## Settings

Global settings live at:

```text
~/.config/slurmctl/settings.json
```

Example:

```json
{
  "editor": "nvim",
  "slurmctl_dir": ".slurmctl"
}
```

`editor` controls which editor opens files for `OO`, `OE`, and `JSON`. If it is
empty, `slurmctl` falls back to `$VISUAL`, `$EDITOR`, `editor`, `nano`, then
`vi`.

`slurmctl_dir` controls where metadata is stored by default. A relative value
like `.slurmctl` is relative to the current working directory. An absolute path
lets you share one metadata store across folders.

CLI overrides win:

```bash
slurmctl --slurmctl-dir /tmp/my-slurmctl show
```

## Metadata

By default, metadata is written under `.slurmctl/`:

```text
.slurmctl/
  commands/
    <command_hash>.json
  runs/
    <run_id>.json
  submissions.jsonl
```

A command is a stable `sbatch` recipe. A run is one actual submission.

Captured data includes:

- submit working directory
- submitted script path
- raw `sbatch` args
- parsed CLI options
- parsed `#SBATCH` directives
- normalized stdout/stderr templates
- resolved stdout/stderr paths
- `sbatch` stdout/stderr/return code
- Slurm job id when available

## Example

The basic example runner submits two jobs:

```bash
slurmctl run examples/basic/runner.sh --dry-run
slurmctl show
```

Expected dry-run output includes two captured submissions with simulated job id
`999999`.
