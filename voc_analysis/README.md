# Steps for VOC analysis

Convert voc csv file to HF unlabelled format
Create a Convoset in HF tool under data management
Upload the data to convoset
Start a new workspace
Clear all the bootstrap intents
Attach the uploaded data to the workspace using data source
Create intents
Then predict all the utterances and produce a prediction csv using predict_utternace_from_voc.py script
Create a useful chart