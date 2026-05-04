#!/bin/sh
#SBATCH --job-name=test_job1
#SBATCH --output=logs/job1-%j.out

sleep(30)
echo "Hello from job1"
