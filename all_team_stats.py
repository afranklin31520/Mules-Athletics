import pandas as pd
import os
from typing import Counter
#load into folder containing all team stats
os.chdir("team_stats")
common_headers = []
num_of_stats = len(os.listdir())
common_headers = Counter()
for csv_file in os.listdir():
    df = pd.read_csv(csv_file)
    df.rename(columns={"Result":"W/L"},inplace=True)
    headers = list(map(lambda x : x.upper() , df.columns[1:]))
    print(csv_file,headers)
    common_headers.update(headers)
max_val = max(common_headers.values())
print(max_val)
#use common headers for insight across all teams with records
common_headers = [column_name for column_name in common_headers if common_headers[column_name] == max_val]
print(common_headers)
new_dfs = pd.concat([pd.read_csv(csv_file).rename(columns={"Result":"W/L"}).rename(str.upper,axis='columns')[common_headers] for csv_file in os.listdir()])
new_dfs.to_csv("all_team_stats.csv")
