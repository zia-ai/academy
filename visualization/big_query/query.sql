WITH ConversationInputs AS (
  SELECT
    t.annotatedConversation.conversation.id AS convoid,
    t.annotatedConversation.conversation.createdAt AS conv_created_at,
    t.annotatedConversation.conversation.updatedAt AS conv_updated_at,
    input.createdAt AS utterance_created_at,
    input.value AS utterance,
    input.source AS role,
    ROW_NUMBER() OVER(PARTITION BY t.annotatedConversation.conversation.id ORDER BY input.createdAt) as row_num
  FROM
    `project_name.dataset_name.table_name` t,
    UNNEST(t.annotatedConversation.conversation.inputs) AS input
),
IntentInputs AS (
  SELECT
    t.annotatedConversation.conversation.id AS convoid,
    intent_input.matches[SAFE_OFFSET(0)].intentId AS intent_id,
    intent_input.matches[SAFE_OFFSET(0)].score AS score,
    ROW_NUMBER() OVER(PARTITION BY t.annotatedConversation.conversation.id) as row_num
  FROM
    `project_name.dataset_name.table_name` t,
    UNNEST(t.annotatedConversation.annotations.inputs_intents.inputs) AS intent_input
)
SELECT
  ci.convoid,
  ci.conv_created_at,
  ci.conv_updated_at,
  ci.utterance_created_at,
  ci.utterance,
  ci.role,
  ii.intent_id,
  ii.score
FROM
  ConversationInputs ci
LEFT JOIN
  IntentInputs ii
ON
  ci.convoid = ii.convoid AND ci.row_num = ii.row_num
ORDER BY
  ci.convoid, ci.utterance_created_at