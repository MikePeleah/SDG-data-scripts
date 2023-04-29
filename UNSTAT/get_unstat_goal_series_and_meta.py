import sys
import re
import os
import json
import requests
import itertools
import csv

def rename_headers(headers, header_mapping):
    """
    Rename headers according to dictionary header_mapping
    """
    mapped_headers = []
    for header in headers:
        for pattern, replacement in header_mapping.items():
            if re.match(pattern, header):
                mapped_headers.append(re.sub(pattern, replacement, header))
                break
        else:
            mapped_headers.append(header)
    return mapped_headers

# -------------------------------------------------------------------------------------- Setting up start -----
# RBEC Countries + EU-27
countries = [   8,  51,  40,  31, 112,  56,  70, 100, 191, 196,
              203, 208, 233, 246, 250, 268, 276, 300, 348, 352,
              372, 380, 398, 412, 417, 428, 438, 440, 442, 470,
              499, 528, 807, 578, 616, 620, 498, 642, 688, 703,
              705, 724, 752, 756, 762, 792, 795, 804, 860 ] # 826, UK out
countries_highligh = [398, 417, 762, 795, 860]

goals = ["1"]
# indicators = ["1.1.1", "1.3.1", "8.6.1", "6.1.1"]
indicators = ["6.1.1"]
series = ["SI_COV_CHLD"]

# Target values 
target_values = [{'6_1_1_SH_H2O_SAFE': {'2025': 85, '2030': 100, }}]

# Set directories for output and input 
output_dir = "C:\\temp\\UNSTAT"
current_dir = os.getcwd()
# -------------------------------------------------------------------------------------- Setting up end -------

# Create folders for outputs -- metadata, config for indicators, data 
os.makedirs(os.path.join(output_dir, "meta"), exist_ok=True)
os.makedirs(os.path.join(output_dir, "indicator-config"), exist_ok=True)
os.makedirs(os.path.join(output_dir, "data"), exist_ok=True)
os.makedirs(os.path.join(output_dir, "tmp"), exist_ok=True)


# Load list of countries with names 
all_geo_fname = os.path.join(output_dir, "Dict of GeoAreas All.json")
if os.path.isfile(all_geo_fname):
    with open(all_geo_fname, "r", encoding="utf-8") as f:
        geo_areas = json.load(f)
else:
# Load from M49 file
    m49_geo_fname = os.path.join(current_dir, "UNSTAT/M49.csv")
    if os.path.isfile(m49_geo_fname):
        geo_areas = []
        # Encoding 'utf-8-sig' to remove BOM see https://stackoverflow.com/questions/17912307/u-ufeff-in-python-string
        with open(m49_geo_fname, "r", encoding="utf-8-sig") as f:
            f_headers = []
            for line in f:
                codes = re.split(r',', line)
                # Read field names
                if not f_headers:
                    f_headers = [c.strip() for c in codes]
                else:
                    m49_geo_temp = {} 
                    for (f_name, value) in zip(f_headers, codes):
                        m49_geo_temp[f_name] = value.strip()
                    geo_areas.append(m49_geo_temp)
    else:
        geo_areas = []
    with open(all_geo_fname, "w", encoding="utf-8") as f:
        json.dump(geo_areas, f, ensure_ascii=False, indent=3)
    

# Get list of all series
all_series_fname = os.path.join(output_dir,"Dict of Series All SDGs.json")
if os.path.isfile(all_series_fname):
    with open(all_series_fname, "r", encoding="utf-8") as f:
        los_d = json.load(f)
else:
    los_req = requests.get("https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/Series/List")
    los = json.loads(los_req.content)
    los_d = {l['code'] : l['description'] for l in los}
    with open(all_series_fname, "w", encoding="utf-8") as f:
        json.dump(los_d, f, ensure_ascii=False, indent=3)

# Get list of all goals, targets and indiocators 
all_gte_fname = os.path.join(output_dir,"Dict of SDG Goals-Targets-Indicators.json")
if os.path.isfile(all_gte_fname):
    with open(all_gte_fname, "r", encoding="utf-8") as f:
        gte_d = json.load(f)
else:
    gte_req = requests.get("https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/Series/List")
    gte_d = json.loads(gte_req.content)
    with open(all_gte_fname, "w", encoding="utf-8") as f:
        json.dump(gte_d, f, ensure_ascii=False, indent=3)

# l_ = [l['indicator'] for l in gte_d]
all_indicators = list(set(item for sublist in [l['indicator'] for l in gte_d] for item in sublist))
# Download'em all
indicators = all_indicators

# indicators = ["6.1.1", "2.5.1"]

"""
headers = { 'accept': 'application/json',  'content-type': 'application/x-www-form-urlencoded', }
skip_meta = ['SDG_INDICATOR_INFO']
if series:
    for s in series:
        print(f"{s} - {los_d.get(s, '-- Unknown --')}")
        params = { 'serieses': s, }
        response = requests.post('https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/SDMXMetadata/GetSDMXMetaData',
                                 params=params,
                                 headers=headers)
        s_meta = response.json()
        with open(f"{s} META.json", "w", encoding="utf-8") as f:
            json.dump(s_meta, f, ensure_ascii=False, indent=3)
        f = open(f"{s} META.yml", "w", encoding="utf-8")
        for m in s_meta:
            if m['conceptId'] not in skip_meta:
                f.write(f"{m['conceptId']}: >-\n  {m['conceptHTML']}\n")
"""

print("That's countries we would like to get:")
for c in countries:
    # Use list comprehension for searching in list of dictionaries, seems fastest  https://stackoverflow.com/questions/8653516/search-a-list-of-dictionaries-in-python
    for geo_ in [x for x in geo_areas if x['M49_code'] == str(c)]:
        print(f"{geo_['M49_code']} - {geo_['ISO3_code']} - {geo_['name_en']} - {geo_['name_ru']}")

# 
headers = { 'accept': 'application/json',  'content-type': 'application/x-www-form-urlencoded', }
skip_meta = ['SDG_INDICATOR_INFO']

# Load template for indicator-config
indicator_config_fname = os.path.join(current_dir,"UNSTAT/indicator-config-yml-template.json")
if os.path.isfile(indicator_config_fname):
    with open(indicator_config_fname, "r", encoding="utf-8") as f:
        indicator_config = json.load(f)
else:
    indicator_config = {}

# Countries to be highlighted by default in the chart 
countries_highligh = [str(c) for c in countries_highligh]
countries_highligh_en = [x['name_en'] for x in geo_areas if x['M49_code'] in countries_highligh]

indicator_titles = []

if indicators:
    for ind in indicators:
        print(f"Processing indicator {ind}")
        dict_ = [x for x in gte_d if ind in x['indicator']]
        for d_ in dict_ :
            print(f"  {d_['code']} - {d_['description']}")
            indicator_titles.append(f"    {ind.replace('.', '-')}_{d_['code']}-title : \'{d_['description']}\'")
            # Get Meta - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            params = { 'serieses': d_['code'], }
            response = requests.post('https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/SDMXMetadata/GetSDMXMetaData',
                                    params=params,
                                    headers=headers)
            s_meta = response.json()
            """
            with open(f"{d_['code']} META.json", "w", encoding="utf-8") as f:
                json.dump(s_meta, f, ensure_ascii=False, indent=3)
            """
            ind_series = f"{ind.replace('.', '-')}_{d_['code']}"
            f = open(os.path.join(output_dir,f"meta/{ind_series}.yml"), "w", encoding="utf-8")
            for m in s_meta:
                if m['conceptId'] not in skip_meta:
                    HTML = m['conceptHTML'].replace('\n','') if m['conceptHTML'] else ""
                    f.write(f"{m['conceptId']}: >-\n  {HTML}\n")
            f.close()

            # Prepare a indicator-config - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
            indicator_config_ = indicator_config
            # Ammend value in template 
            indicator_config_['graph_title'] = f"global_indicators.{ind_series}-title"
            indicator_config_['indicator_name'] = f"global_indicators.{ind_series}-title"
            indicator_config_['indicator_number'] = f"{ind_series}"
            indicator_config_['permalink'] = f"\'/{ind_series}/\'"
            indicator_config_['notstarted'] = "reported"
            indicator_config_['tag'] = "\n  - custom.regional"
            start_value_str = "\n  - field: GeoAreaName\n    value: "
            indicator_config_['data_start_values'] = start_value_str + start_value_str.join(countries_highligh_en) + "\n"
            # Add target values 
            tv_ind = [x[ind] for x in target_values if ind in x.keys()]
            target_ = ''
            if tv_ind:
                # Assume only one set of lines for an indicator
                for k in tv_ind[0].keys():
                    target_ = target_ + f"\n  - series: \'\'\n    unit: \'\'\n    label_content: indicator.annotation_{k}_target\n    value: {tv_ind[0].get(k)}"
            else:
                target_ = '[]'
            indicator_config_['graph_target_lines'] = target_
            # Dump indicator-config into file
            f = open(os.path.join(output_dir, f"indicator-config/{ind_series}.yml"), "w", encoding="utf-8")
            for ind_k in indicator_config_.keys():
                f.write(f"{ind_k} : {indicator_config_[ind_k]}\n")
            f.close()

            # Load data - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            headers = {
                        'accept': 'application/json',
                        # requests won't add a boundary if this header is set when you pass files=
                        # 'Content-Type': 'multipart/form-data',
                      }
            files = [('areaCodes', (None, str(c))) for c in countries]
            files.extend([('seriesCodes', (None, d_['code'])),
                          ('timePeriodStart', (None, '')),
                          ('timePeriodEnd', (None, '')),
                         ])
            # Check if dump exists, load from it, else get from site 
            fname_dump = os.path.join(output_dir,f"tmp/response_dump_{d_['code']}")
            if os.path.isfile(fname_dump):
                with open(fname_dump, 'rb') as f:
                    content = f.read().decode("utf-8")
            else:
                response = requests.post('https://unstats.un.org/sdgs/UNSDGAPIV5/v1/sdg/Series/DataCSV', headers=headers, files=files)
                with open(fname_dump, 'wb') as f:
                    f.write(response.content)
                content = response.content.decode("utf-8")
            # OpenSDG requires csv file to have TimePeriod in the left most column and Value in the rightmost, breakdown variables are in the middle 
            # This code mapping collumns from responce csv into format required by OpenSDG 
            header_pattern = [r'TimePeriod', r'GeoAreaName', r'\[.*', r'Value']
            header_drop = ['[Nature]', '[Observation Status]', '[Reporting Type]', '[Units]']
            header_mapping = {
                r'^TimePeriod$': 'Year',
                r'^\[(.*?)\]$': r'\1'
            }
            content_rows = csv.reader([row.strip().strip('\x00') for row in content.split('\n')])
            filtered_rows = []
            header_indexes = None

            if content_rows:
                for content_row in content_rows:
                    row = content_row
                    if header_indexes is None:
                        header_indexes = [index for pattern in header_pattern for index, header in enumerate(row) if (re.match(pattern, header) and header not in header_drop)]
                        selected_row = rename_headers([row[i] for i in header_indexes], header_mapping)
                    # Check for behaviour when getting only header, no values
                    else:
                        if row:
                            selected_row = [row[i] for i in header_indexes]
                    # Skip rows with NaN in data
                    if selected_row[-1] != 'NaN':
                        filtered_rows.append(selected_row)
            with open(os.path.join(output_dir,f"data/indicator_{ind_series}.csv"), 'w', encoding='utf-8') as f:
                f.write("\n".join([",".join(row) for row in filtered_rows]))
    with open(os.path.join(output_dir,f"translations_add.yml"), 'w', encoding='utf-8') as f:
        f.write("en:\n  global_indicators:\n")
        f.write("\n".join(indicator_titles))

print("* * * That's all, folks! * * *")
