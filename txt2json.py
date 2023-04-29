import json
import re

template = {}
fname = "indicator-config-yml-template"
with open(f"{fname}.txt", "r", encoding="utf-8-sig") as f:
    for line in f:
        codes = re.split(r':', line)
        template[codes[0].strip()] = codes[1].strip()

with open(f"{fname}.json", "w", encoding="utf-8") as f:
    json.dump(template, f, ensure_ascii=False, indent=3)


print("* * * That's all, folks! * * *")
