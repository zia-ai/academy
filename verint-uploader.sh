#!/usr/bin/env bash
# verint-uploader.sh <namespace> <playbook_id> <directory>
# Make sure you have authorised using hf auth login first
if [ "$1" == "" ]
  then
    echo "Script requires name space to load into"
    exit
fi
NAME_SPACE=$1

if [ "$2" == "" ]
  then
    echo "Script requires workspace playbook-ID to load into"
    exit
fi
WORKSPACE_ID=$2

if [ "$3" == "" ]
  then
    echo "Script requires directory to search for files in"
    exit
fi
DIRECTORY=$3

# Check on correct name space 
hf namespace use $NAME_SPACE

echo Will try and load all -hf.json in $DIRECTORY

FILENAMES=`ls $DIRECTORY/*-hf.json`
for FILE in $FILENAMES
do
   echo `hf conversation import --workspace $WORKSPACE_ID --format json $FILE`
done

