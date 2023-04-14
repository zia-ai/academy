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
For multidimensional analysis use bubble_chart.py
Fine tuning the model, especially aspects negative, based on the background of the customers
 - enrich the background intent
 - make batch predict
 - create a script that adds metadata to the document containing utterances that are predicted as background
 - upload the script to the HF tool
 - filter utterances using metadata and find negative aspects from these utterances and move them to the stash.
 - remove the filter
 - find similar utterances from the entire dataset
 - create new intents in the negative aspects
 - Again make batch predict
 - create the chart
   - xaxis background intents
   - yaxis volume of utterances
   - inside chart - negative aspects

Does the DET model in HF Studio need strengthening?
Check the background model disabled/kids/seniors/other
Relabelling the conversations with a disabled/kids/seniors/other metadata tag and reuploading as a new dataset
Changing the model to point at that and filtering for conversations for the three specialist are there additional AN that particular affect that group.
Strenghten any training needed and add any new intents.
Having gone through and improved model for those groups
How could we visualise the top issues for VoC for Disabled, Parents with Kids and Seniors, compared to other groups not identifying in their feedback?
I.e if we particularly wanted to understand what affected those groups without asking an age question in the