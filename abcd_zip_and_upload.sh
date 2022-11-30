#!/usr/bin/env bash
# abcd_zip_and_upload.sh stephen-demo abcd abcd_unlabelled05.json
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
    echo "Script requires filename to zip and load"
    exit
fi
FILENAME=$3

# Check on correct name space needs to be parameterised
hf namespace use $NAME_SPACE

# clear out old zip
rm ./data/$FILENAME.gz

# cycle through any unlablled json validating them, printing number of records and creating zips
echo `hf conversation validate --format json $FILENAME`
echo "gzip $FILENAME"
gzip --best --keep --force "$FILENAME"
echo `hf conversation import --workspace $WORKSPACE_ID --format json $FILENAME.gz`
