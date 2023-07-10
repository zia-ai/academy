#!/bin/bash
SCRIPT_PATH=$(dirname -- "$(readlink -f -- "$BASH_SOURCE")")
LOG_PATH=$SCRIPT_PATH/logs
SUMMARIES_PATH=$SCRIPT_PATH/summaries
find "${LOG_PATH}" -name "*.log" -delete
find "${SUMMARIES_PATH}" -name "*.txt" -delete