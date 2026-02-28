import csv
import json
import os

csv_file_path = 'data/transactions.csv'    
json_file_path = 'json_lab/transactions.json'  

data = []
with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)  
    for row in reader:
        data.append(row)

with open(json_file_path, 'w', encoding='utf-8') as jsonfile:
    json.dump(data, jsonfile, indent=4, ensure_ascii=False)

print(f"converted file : {json_file_path}")