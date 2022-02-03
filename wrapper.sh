#!/bin/bash
python3 ./worker/worker.py &
python3 ./producer/app.py &

wait -n

exit $?
