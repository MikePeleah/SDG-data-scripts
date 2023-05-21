#!/usr/bin/env python
# coding: utf-8

# In[53]:


import requests
import os
import json
import glob
import shutil


# In[54]:


indicator_series=['SI.POV.DDAY', 'SI.POV.LMIC.GP', 'SI.POV.UMIC', 'SI.POV.MDIM', 'SI.POV.MDIM.XQ', 'SI.POV.NAHC']
country_codes=['KAZ','KGZ','TJK','TKM','UZB']
r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json'.format(country_codes[4],indicator_series[1]))
pages=r.json()[0]['pages']
print(pages)


# In[55]:


parent=os.getcwd()
directory='SDG-DATA'
path = os.path.join(parent, directory)
#make sub-directory for metadata files within SDG Data
try:
    os.makedirs('{0}/Json_MetaData/'.format(path))
    os.makedirs('{0}/Yaml_MetaData/'.format(path))
except:
    pass


# In[56]:


path


# In[57]:


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
        try:
            new_sub_dir=os.makedirs('{0}/Json_{1}_{2}/'.format(path,country,indicator)) 
        except Exception as e:
            print('Error {0}'.format(str(e)))
            pass
        #retrieve data for each page separately
        for page in range(pages):
            page=page+1
            r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json&page={2}'.format(country,indicator,page),headers=headers)
            data=r.json()[1]
            file=os.path.join('{0}\Json_{1}_{2}'.format(path,country,indicator),"Data_{0}_{1}_page_{2}.json".format(country,indicator,page))
            with open(file, "w") as outfile:
                json.dump(data, outfile)
                outfile.close()
    
    


# In[58]:


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


# In[59]:


#Getting metadata in Json and Converting it to Yaml


# In[60]:


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
    


# In[61]:


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


# In[62]:


##load and convert all metadata


# In[63]:


#listing metafile
metafiles=glob.glob(r'{0}/*json'.format(meta_path))


# In[64]:


#Processing Json files into Yaml
import yaml
for jsonfile in metafiles:
    file=open('{0}'.format(jsonfile),"r")
    python_dict=json.load(file)
    jsonfilename=jsonfile.split(".")[0]+jsonfile.split(".")[1]+jsonfile.split(".")[2]
    file1=open('{0}.yaml'.format(jsonfilename),"w")
    yaml.dump(python_dict,file1)
    file1.close()


# In[65]:


#moving all yaml files to Yaml subdirectory


# In[66]:


for file in os.listdir(meta_path):
    ext=os.path.splitext(file)[-1]
    if ext =='.yaml':
        shutil.move(os.path.join('{0}'.format(meta_path),file), os.path.join('{0}'.format(yaml_path),file))


# In[67]:


#processing Json files into excel csv


# In[68]:


#load each json file and compare the country code of the file
import glob


# In[69]:


#Removing Meta files from original list of files
Files=[]
for file in glob.glob('{0}/*/'.format(path), recursive = True):
    if 'Meta' in file :
        pass
    else:
        Files.append(file)


# In[70]:


Files


# In[71]:


for subdir in Files:
    file_list=os.listdir(subdir)
    result = []
    for i in range(len(file_list)):
        f=os.path.join(path,subdir,file_list[i])
        with open(os.path.join(path,f)) as f1:
            result.append(json.load(f1))
    with open(os.path.join(path,subdir,"{0}_merged_file.json".format(file_list[i].split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0])) ,"w") as outfile:
        json.dump(result, outfile)


# In[72]:


#Read all merged Json files and covert them into Csvs into their respective subfolder


# In[73]:


import pandas as pd


# In[74]:


for subdir in Files:
    file_list=os.listdir(subdir)
    for file in file_list:
        if 'merged' in file:
            file_path=os.path.join(path,subdir,"{0}.json".format(file.split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0]))
    with open(file_path,'r') as k:
        df=pd.DataFrame()
        l=json.load(k)
        df_list=[]
        file_name=os.path.join(path,subdir,"{0}".format(file.split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0]))
    for i in range(len(l)):
        object_=l[i]
        df_list.extend(object_)
        df=pd.DataFrame.from_records(pd.json_normalize(df_list))
        df.to_csv(os.path.join(path,subdir,"{0}.csv".format(file_name)))


# In[ ]:




