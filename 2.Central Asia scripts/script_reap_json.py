# -*- coding: utf-8 -*-
"""UNDP - SDG.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1Padt7CHH5_orIKIJZ3CsSLk-xfNBi5YF
"""

import requests
inport os
import json

indicator_series=['SI.POV.DDAY', 'SI.POV.LMIC.GP', 'SI.POV.UMIC', 'SI.POV.MDIM', 'SI.POV.MDIM.XQ', 'SI.POV.NAHC']
country_codes=['KAZ','KGZ','TJK','TKM','UZB']
r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json'.format(country_codes[4],indicator_series[1]))
pages=r.json()[0]['pages']
print(pages)

parent=os.getcwd()
directory='SDG-DATA'
path = os.path.join(parent, directory)

# code to prevent server from Connection reset 10054 error
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}

for country in countries:
    #send request to grab number of pages for the data
    for indicator in indicator_series:
        req=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json'.format(country,indicator),headers=headers)
        pages=req.json()[0]['pages']
        #retrieve data for each page separately
        for page in range(pages):
            page=page+1
            r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json&page={2}'.format(country,indicator,page),headers=headers)
            data=r.json()[1]
            file=os.path.join(path,"Data_{0}_{1}_page_{2}.json".format(country,indicator,page))
            with open(file, "w") as outfile:
                json.dump(data, outfile)
            out_file.close()

#listing files for easy access for further processing
json_files=os.listdir(path)
f = open(os.path.join(path,'Data_KG_SI.POV.DDAY_page_2.json'))
# loading json
data = json.load(f)

#retrieve metadata

for country in countries:
    #send request to grab number of pages for the data
    for indicator in indicator_series:
        req=requests.get('http://api.worldbank.org/v2/sources/2/country/{0}/series/{1}/metadata?format=json'.format(country,indicator),headers=headers)
        meta=req.json()
        file=os.path.join(path,"MetaData_{0}_{1}.json".format(country,indicator))
        with open(file, "w") as outfile:
            json.dump(meta, outfile)
            out_file.close()

f = open(os.path.join(path,'MetaData_KZ_SI.POV.DDAY.json'))
# loading metadataa
metadata = json.load(f)

f.close()

