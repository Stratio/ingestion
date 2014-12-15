#!/bin/bash
getent hosts kibana
getent hosts elasticsearch
exec /demo/bin/run_example.sh
