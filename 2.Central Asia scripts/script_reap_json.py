#!/usr/bin/env python
# coding: utf-8

# In[16]:


import requests
import os
import json
import glob
import shutil


# In[17]:


indicator_series=['SI.POV.DDAY', 'SI.POV.LMIC.GP', 'SI.POV.UMIC', 'SI.POV.MDIM', 'SI.POV.MDIM.XQ', 'SI.POV.NAHC']
country_codes=['KAZ','KGZ','TJK','TKM','UZB']
r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json'.format(country_codes[4],indicator_series[1]))
pages=r.json()[0]['pages']
print(pages)


# In[18]:


parent=os.getcwd()
directory='SDG-DATA'
path = os.path.join(parent, directory)
#make sub-directory for metadata files within SDG Data
try:
    os.makedirs('{0}/Json_MetaData/'.format(path))
    os.makedirs('{0}/Yaml_MetaData/'.format(path))
except:
    pass


# In[19]:


# code to prevent server from Connection reset 10054 error
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"
}

for country in country_codes:
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
                outfile.close()
    


# In[21]:


#opening sample json
#listing json files for easy access for further processing
json_files=os.listdir(path)
try:
    f = open(os.path.join(path,'Data_KGZ_SI.POV.DDAY_page_2.json'))
    # loading file
    data = json.load(f)
    f.close()
except:
    pass


# In[23]:


#Getting metadata in Json and Converting it to Yaml


# In[24]:


#creating a path to sub-directory for metadata
meta_path=os.path.join(path,'Json_MetaData')
yaml_path=os.path.join(path,'Yaml_MetaData')

for country in country_codes:
    #send request to grab number of pages for the data
    for indicator in indicator_series:
        req=requests.get('http://api.worldbank.org/v2/sources/2/country/{0}/series/{1}/metadata?format=json'.format(country,indicator),headers=headers)
        meta=req.json()
#         file=os.path.join(meta_path,"MetaData_{0}_{1}.json".format(country,indicator))
        with open(os.path.join(meta_path,"MetaData_{0}_{1}.json".format(country,indicator)), "w") as outfile:
            json.dump(meta, outfile)
            outfile.close()
    


# In[26]:


#loading and checking files
try:
    f = open(os.path.join(meta_path,'MetaData_KAZ_SI.POV.DDAY.json'))
    # loading metadata for viewing
    metadata = json.load(f)
    #closing file
    f.close()
except:
    print('Error reading files')
    pass


# In[27]:


##load and convert all metadata


# In[28]:


#listing metafile
metafiles=glob.glob(r'{0}/*json'.format(meta_path))


# In[29]:


#Processing Json files into Yaml
import yaml
for jsonfile in metafiles:
    file=open('{0}'.format(jsonfile),"r")
    python_dict=json.load(file)
    jsonfilename=jsonfile.split(".")[0]+jsonfile.split(".")[1]+jsonfile.split(".")[2]
    file1=open('{0}.yaml'.format(jsonfilename),"w")
    yaml.dump(python_dict,file1)
    file1.close()


# In[30]:


#moving all yaml files to Yaml subdirectory


# In[31]:


for file in os.listdir(meta_path):
    ext=os.path.splitext(file)[-1]
    if ext =='.yaml':
        shutil.move(os.path.join('{0}'.format(meta_path),file), os.path.join('{0}'.format(yaml_path),file))


# In[ ]:





# In[ ]:




