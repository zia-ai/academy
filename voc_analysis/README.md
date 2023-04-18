# Steps for VOC analysis

## Single dimensional and multidimensional analysis
Convert voc csv file to HF unlabelled format using voc_csv_to_hf_unlabelled.py and make sure to use sentence_split flag while running the script 
Create a Convoset in HF tool under data management
Upload the data to convoset
Start a new workspace
Clear all the bootstrap intents
Attach the uploaded data to the workspace using data source
Create intents (entities optional - not required for this analysis)
Then predict all the utterances and produce a prediction csv using predict_utternace_from_voc.py script. Using background flag is not required at this stage of analysis
Create a useful chart - pie chart, timeseries, bar chart, etc
For multidimensional analysis use bubble_chart.py. This produces bubble chart (avg nps score v churn risk indicator score v issues)

## Following steps are for background analysis
Fine tuning the model, especially aspects negative, based on the background of the customers
 - enrich the background intent
 - make batch predict using predict_utternace_from_voc.py. Make sure to include background flag while running the script. This includes background information of reviewers or document owners
 - Convert the prediction csv file produced in the previous step into HF unlabelled json format using voc_csv_to_hf_unlabelled.py. This time do not include sentence_split flag as all the documents are already split into utterances in the prediction csv
 - upload the HF unlabelled json into HF tool 
 - filter utterances using background metadata and find negative aspects from these utterances and move them to the stash.
 - remove the filter
 - find similar utterances from the entire dataset
 - create new intents in the negative aspects
 - Again make batch predict
Create charts for the prediction csv as per your needs. For example, Issues faced by different group of people - visualising the top issues for VoC for Disabled, Parents with Kids and Seniors, compared to other groups not identifying in their feedback
I.e if we particularly wanted to understand what affected those groups without asking an age question