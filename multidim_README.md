# Multidim testing
Assumes have access to multidim namespace and the ABCD raw files from HF datasets

Assumes 1 convoset with n playbooks, with n pipelines.  Each pipeline being for a different dimension of analysis.

TODO: where does data live - scripts to transform here, but not very useful. 
Just check it in - makes thing huge?
Zip and put somewhere?

IN the document to insight folder

## implements this logic (sequential)

Upload file to conversation set.
- hf_api.get_conversation_set
- hf_api.upload_json_file_to_conversation_source

Check on each upload file is loaded and jobs all complete before starting pipelines
- loop until TRIGGER_STATUS_COMPLETED
- - hf_api.describe_trigger

Workout all playbooks to run
- hf_api.list_playbooks

For each playbook (workspace) get the pipelines for that playbook
- hf_api.list_playbook_pipelines

Run them one by one 
- - hf_api.trigger_playbook_pipeline

Check for each that pipeline finishes
- - loop until TRIGGER_STATUS_COMPLETED
- - - hf_api.describe_trigger

Download the results filtering by filename as a metadata key immediately
- - hf_api.export_query_conversation_inputs

## implements this logic (parallel)

Upload sinlge file to conversation set (skippable)
- hf_api.get_conversation_set
- hf_api.upload_json_file_to_conversation_source

Doesn't check that trigger finished

Workout all playbooks to run
- hf_api.list_playbooks

For each playbook (workspace) get the pipelines for that playbook
- hf_api.list_playbook_pipelines

Run them one by one - without waiting for triggers to finsih
- hf_api.trigger_playbook_pipeline

Download the results filtering by filename as a metadata key immediately for every playbook
- hf_api.export_query_conversation_inputs