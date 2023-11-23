import pandas
import json


file = open("/home/ubuntu/source/academy/summarize/workspaces/summarize_summaries_aprende.json", mode = "r", encoding = "utf8")
workspace_json = json.load(file)
print(workspace_json)
file.close()
df = pandas.json_normalize(workspace_json["examples"])
print(df)

df.to_csv("/home/ubuntu/source/academy/data/Aprende/objections.csv",index=False, header=True)