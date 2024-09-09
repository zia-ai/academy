-- Example where data is loaded with the timezone and bigquery stores as UTC
-- Then this query reads it back in the right ISO8601 text format in for example
-- Australia/Sydney
-- Change names to be text and add roles
-- to support a Union all
-- TODO some input params at top that are easy to change.
SELECT inputs.session_id, inputs.created_at, inputs.role, inputs.text
FROM
  (
    SELECT 
      `sessionId` as session_id,
      CAST(
        FORMAT_DATETIME("%Y-%m-%dT%H:%M:%E3S%z",`sydneyDateTime`,'Australia/Sydney') 
      AS STRING) as created_at,
      `Input Query` as text,
      'client' as role
    FROM `<some_table>`
    -- we drop any empty lines - for instance the first input is always blank
    WHERE `Input Query` != ""
    -- TODO: some date ranging
  ) as inputs
UNION ALL
SELECT responses.session_id, responses.created_at, responses.role, responses.text
FROM
  (
    SELECT  
      `sessionId`as session_id,
      -- here we add a millisecond to the response so it always come after the input
      CAST(
        FORMAT_DATETIME(
          "%Y-%m-%dT%H:%M:%E3S%z",
          DATETIME_ADD(`sydneyDateTime`,INTERVAL 1 MILLISECOND)
          ,'Australia/Sydney') 
        AS STRING) as created_at,
      `Response Text` as text,
      'expert' as role
    FROM `<some_table>` 
    WHERE `Response Text` != ""
  ) as responses
ORDER BY session_id, created_at ASC