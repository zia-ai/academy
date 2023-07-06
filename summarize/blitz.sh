#!/bin/bash
SCRIPT_PATH=$(dirname -- "$(readlink -f -- "$BASH_SOURCE")")
echo $SCRIPT_PATH
rm $SCRIPT_PATH/logs/*.log
rm $SCRIPT_PATH/summaries/*.txt