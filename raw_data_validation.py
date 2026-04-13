import json

file_path = "data/raw/Kindle_Store_5/Kindle_Store_5.json"

count = 0

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        try:
            data = json.loads(line)
            count += 1
        except:
            pass

print("Total rows:", count)
