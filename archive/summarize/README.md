# Steps for summarization

Pre-requites:
 - Have conversations/transcripts in HF JSON format
 - Have OpenAI API key

## Step1:

 - Summarize all the conversations (or n number of conversations) using summarize_transcripts.py
 - This would summarize all the conversations under 4k context window limit (prompt + completion).
 - Produces the summaries of each conversation
 - IDs of conversations that did not get summarized because of large context length (more than 4k) are also produced as output.

## Step2: Only required if there are any large conversations that failed to get summarized in the previous step.

 - Summarize large conversations using summarize_long_transcripts.py
 - Conversations are segmented into multiple parts with each segment having not more than 1800 tokens
 - First segment is processed using prompt1.txt and prompt2.txt
 - Rest of the segments are processed using prompt3.txt, prompt4.txt and prompt5.txt
 - Produces summaries after processing each segment. Summary containing all the info is the final summary produced after processing nth segment.

## Step3:

 - Process all the summaries using process_summaries.py
 - Produces the summaries in HF JSON format