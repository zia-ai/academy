curl --request POST \
  --url 'https://api.deepgram.com/v1/listen?model=whisper-large&diarize=true&punctuate=true&smart_format=true' \
  --header "Authorization: Token $DG_TOKEN" \
  --header 'content-type: audio/mpeg' \
  --data-binary "@./data/Radio Charades Episode 3 Whats your game_00m_00s__10m_00s.mp3" \
  --output "./data/dg_whisper.json"