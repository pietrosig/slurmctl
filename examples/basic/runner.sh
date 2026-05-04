#!/bin/sh
set -eu

cd "$(dirname "$0")"

sbatch job1.sh
sbatch --output logs/job2-%j.out --error logs/job2-%j.err --job-name test_job2 job2.sh
