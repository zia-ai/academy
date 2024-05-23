import pandas
import json
df = pandas.json_normalize(json.load(open('abcd_nps_and_resolution_added_2022_05 (1).json',encoding='utf8'))["examples"])
print(df)
print(df.columns)
assert isinstance(df,pandas.DataFrame)
gb = df[["context.role","id"]].groupby("context.role").count()
print(gb)