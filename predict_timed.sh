curl -L -X POST "https://api.humanfirst.ai/v1alpha1/nlu/predict/$NAMESPACE/$PLAYBOOK" \
-H "Authorization: Bearer $BEARER_TOKEN" \
-H "Content-Type: application/json; charset=utf-8" \
-H "Accept: application/json" \
--w "@curl-format.txt" \
--data-raw "{\"namespace\": \"$NAMESPACE\",\"playbook_id\": \"$PLAYBOOK\",\"input_utterance\": \"Hello Charlie how fast can you go?\"}}"