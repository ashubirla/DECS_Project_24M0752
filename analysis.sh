#!/bin/bash
DURATION=${1:-300}
mkdir -p perf_logs

echo "Recording for $DURATION seconds..."
mpstat -P 1 1 $DURATION > perf_logs/mpstat.txt &
iostat -xz 1 $DURATION > perf_logs/iostat.txt &

wait
echo "Logs saved to perf_logs/"