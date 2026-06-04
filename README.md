# slurmctl

`slurmctl` is a minimal, dependency-free Python CLI for intercepting `sbatch`
calls made by your runner scripts.

It works by putting a temporary fake `sbatch` at the front of `PATH`. The fake
`sbatch` parses the submitted command and script directives, calls the real
`sbatch` when available, captures the Slurm job id, and writes metadata to a
shared `~/.slurmctl` directory by default.

## Requirements

- Python 3.11+
- Standard library only
- Slurm tools for real submissions: `sbatch`, `squeue`, `scancel`
- Optional Slurm accounting for recently finished jobs: `sacct`

`--dry-run` works without Slurm installed.

## Install

Recommended install:

```bash
curl -fsSL https://raw.githubusercontent.com/pietrosig/slurmctl/main/install.sh | sh
```

Manual single-file install:

```bash
mkdir -p ~/.local/bin
curl -fsSL https://github.com/pietrosig/slurmctl/releases/latest/download/slurmctl -o ~/.local/bin/slurmctl
chmod +x ~/.local/bin/slurmctl
```

The installed `slurmctl` launcher looks for `python3.13`, `python3.12`,
`python3.11`, then `python3`, and only uses an interpreter if it is Python
3.11+. You can force a specific interpreter with `SLURMCTL_PYTHON`.

Make sure `~/.local/bin` is on your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Update an installed release:

```bash
slurmctl update
```

Check for an update without changing the installed executable:

```bash
slurmctl update --check
```

Optional plain `sbatch` capture:

```bash
slurmctl
```

Open **install sbatch** from the home screen, or **settings** then
**sbatch wrapper**. This installs a managed `~/.local/bin/sbatch` wrapper so
plain `sbatch ...` behaves like `slurmctl sbatch ...`; scripts no longer need
to be launched through `slurmctl run` just to capture submissions.
`slurmctl run` remains useful for temporary interception without installing
the wrapper.

Make sure `~/.local/bin` appears before the real Slurm `sbatch` directory on
`PATH`. The wrapper stores the real `sbatch` path and `slurmctl` also skips its
own managed wrapper during discovery, so calls do not recurse back into
`slurmctl`.

For local development from this repo:

```bash
chmod +x slurmctl.py
./slurmctl.py doctor
```

## Release

`slurmctl update` installs the latest GitHub Release asset named `slurmctl`.

The release workflow copies `slurmctl.py` to an executable asset named
`slurmctl` and attaches it to the GitHub Release.

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

### `sbatch`

```bash
slurmctl sbatch [SBATCH_OPTIONS...] SCRIPT [SCRIPT_ARGS...]
```

Submits one `sbatch` recipe directly while capturing the same metadata as
intercepted submissions.

Examples:

```bash
slurmctl --dry-run sbatch examples/basic/job1.sh
slurmctl sbatch --output logs/%j.out --job-name test examples/basic/job2.sh
```

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
jobs that finished in the last 24 hours. If `squeue` is not installed, it falls
back to captured submissions so the view still works off-cluster.

### `doctor`

```bash
slurmctl doctor
```

Prints detected Slurm tools, settings path, editor, and metadata directory.
It also reports whether the optional managed `sbatch` wrapper is installed and
active on `PATH`.

## Interactive Shell

Start with:

```bash
slurmctl
```

Home views:

- `run`: select or type a shell script to run
- `show`: browse captured submissions
- `watch`: browse live Slurm jobs or captured fallback jobs
- `install sbatch`: install or remove optional plain `sbatch` capture
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
- `v`: start or clear visual selection
- Arrow up/down or `j`/`k`: extend visual selection
- Enter while visually selecting: open selected-jobs actions
- selected-jobs actions: cancel selected live jobs or rerun selected captured recipes

The interactive `watch` view caches Slurm results briefly so large job lists do
not make every redraw slow. Press `R` in the watch list to refresh manually.
Select `Load more` at the end of the finished jobs to expand the `sacct` window
by 12 hours.

Cancel and rerun actions ask for yes/false confirmation before running. If a
captured rerun recipe has a Slurm dependency, rerun asks whether to keep the
exact recipe or remove dependency options. With `--dry-run`, cancel only reports
what would happen.

## Settings

Global settings live at:

```text
~/.config/slurmctl/settings.json
```

Example:

```json
{
  "editor": "nvim",
  "slurmctl_dir": "/home/you/.slurmctl"
}
```

`editor` controls which editor opens files for `OO`, `OE`, and `JSON`. If it is
empty, `slurmctl` falls back to `$VISUAL`, `$EDITOR`, `editor`, `nano`, then
`vi`.

`slurmctl_dir` controls where metadata is stored by default. The default is
`$HOME/.slurmctl`, so captured jobs are shared across folders. A relative value
is still accepted and is relative to the current working directory.

CLI overrides win:

```bash
slurmctl --slurmctl-dir /tmp/my-slurmctl show
```

## Metadata

By default, metadata is written under `$HOME/.slurmctl/`:

```text
~/.slurmctl/
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
