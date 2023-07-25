#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import os
import json
import glob
import shutil


# In[137]:


indicator_series=['SI.POV.DDAY',
'SI.POV.LMIC.GP',
'SI.POV.UMIC',
'SI.POV.MDIM',
'SI.POV.MDIM.XQ',
'SI.POV.NAHC',
'SH.H2O.SMDW.ZS',
'SH.STA.SMSS.ZS',
'EG.ELC.ACCS.ZS',
'SH.XPD.GHED.GE.ZS',
'SE.XPD.TOTL.GB.ZS',
'NV.AGR.EMPL.KD',
'AG.YLD.CREL.KG',
'SE.PRE.ENRR',
'SE.PRE.ENRR.FE',
'SE.PRE.ENRR.MA',
'SE.TER.ENRR',
'SE.TER.ENRR.FE',
'SE.TER.ENRR.MA',
'SE.ENR.PRIM.FM.ZS',
'SE.ENR.SECO.FM.ZS',
'SE.ENR.TERT.FM.ZS',
'SE.ADT.LITR.ZS',
'SE.ADT.LITR.FE.ZS',
'SE.ADT.LITR.MA.ZS',
'SE.ADT.1524.LT.ZS',
'SE.ADT.1524.LT.FE.ZS',
'SE.ADT.1524.LT.MA.ZS',
'IC.FRM.FEMO.ZS',
'ER.GDP.FWTL.M3.KD',
'ER.H2O.FWTL.ZS',
'EG.USE.ELEC.KH.PC',
'EG.ELC.RNEW.ZS',
'SL.GDP.PCAP.EM.KD',
'ST.INT.RCPT.XP.ZS',
'SI.POV.GINI',
#'PALMA.INQ',
'EN.ATM.PM25.MC.M3',
'EN.ATM.PM25.MC.T1.ZS',
'EN.ATM.PM25.MC.T2.ZS',
'EN.ATM.PM25.MC.T3.ZS',
'EN.ATM.PM25.MC.ZS',
'GC.REV.XGRT.GD.ZS',
'GC.REV.GOTR.ZS',
'BX.KLT.DINV.WD.GD.ZS',
'NE.EXP.GNFS.ZS',
'GC.TAX.TOTL.GD.ZS',
'DT.ODA.ODAT.GN.ZS',
'BX.TRF.PWKR.DT.GD.ZS',
'SP.REG.BRTH.ZS',
'SP.REG.DTHS.ZS',
'IQ.SCI.OVRL',
'IQ.SCI.MTHD',
'IQ.SCI.SRCE',
'IQ.SCI.PRDC']
country_codes=['KAZ','KGZ','TJK','TKM','UZB']
r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json'.format(country_codes[4],indicator_series[1]))
pages=r.json()[0]['pages']
print(pages)


# In[138]:


parent=os.getcwd()
directory='SDG-DATA'
path = os.path.join(parent, directory)
#make sub-directory for metadata files within SDG Data
try:
    os.makedirs('{0}/Json_MetaData/'.format(path))
    os.makedirs('{0}/Yaml_MetaData/'.format(path))
    os.makedirs('{0}/Extra_Data/'.format(path))
except:
    pass


# In[139]:


path


# In[141]:


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
        #retrieve data for each page separately
        for page in range(pages):
            page=page+1
            r=requests.get('http://api.worldbank.org/v2/country/{0}/indicator/{1}?format=json&page={2}'.format(country,indicator,page),headers=headers)
            data=r.json()[1]
            file=os.path.join('{0}\Json_{1}_{2}'.format(path,country,indicator),"Data_{0}_{1}_page_{2}.json".format(country,indicator,page))
            with open(file, "w") as outfile:
                json.dump(data, outfile)
                outfile.close()
    
    


# In[142]:


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


# In[143]:


#Getting metadata in Json and Converting it to Yaml


# In[144]:


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
    


# In[145]:


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


# In[146]:


##load and convert all metadata


# In[147]:


#listing metafile
metafiles=glob.glob(r'{0}/*json'.format(meta_path))


# In[148]:


#Processing Json files into Yaml
import yaml
for jsonfile in metafiles:
    file=open('{0}'.format(jsonfile),"r")
    python_dict=json.load(file)
    jsonfilename=jsonfile.split(".")[0]+jsonfile.split(".")[1]+jsonfile.split(".")[2]
    file1=open('{0}.yaml'.format(jsonfilename),"w")
    yaml.dump(python_dict,file1)
    file1.close()


# In[149]:


#moving all yaml files to Yaml subdirectory


# In[150]:


for file in os.listdir(meta_path):
    ext=os.path.splitext(file)[-1]
    if ext =='.yaml':
        shutil.move(os.path.join('{0}'.format(meta_path),file), os.path.join('{0}'.format(yaml_path),file))


# In[151]:


#processing Json files into excel csv


# In[152]:


#load each json file and compare the country code of the file
import glob


# In[153]:


#Removing Meta files from original list of files
Files=[]
for file in glob.glob('{0}/*/'.format(path), recursive = True):
    if 'Meta' in file :
        pass
    else:
        Files.append(file)


# In[154]:


Files


# In[155]:


for subdir in Files:
    file_list=os.listdir(subdir)
    result = []
    for i in range(len(file_list)):
        f=os.path.join(path,subdir,file_list[i])
        with open(os.path.join(path,f)) as f1:
            result.append(json.load(f1))
        with open(os.path.join(path,subdir,"{0}_merged_file.json".format(file_list[i].split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0])) ,"w") as outfile:
            json.dump(result, outfile)


# In[156]:


#Read all merged Json files and covert them into Csvs into their respective subfolder


# In[157]:


import pandas as pd


# In[158]:


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


# In[159]:


#merge all files for same indicator into one major file
import re
import shutil


# In[160]:


for subdir in Files:
    file_list=os.listdir(subdir)
    for file in file_list:
        ext=os.path.splitext(file)[-1]
        if ext== '.csv':
            print(file)
            file_name=os.path.join(path,subdir,"{0}".format(file.split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0]))
            indicator=os.path.splitext(file)[0].split('_')[1]
            try:
                new_sub_dir=os.makedirs('{0}/Master_Data_Indicator_{1}/'.format(path,indicator)) 
            except Exception as e:
                pass


# In[161]:


#Removing Meta files from original list of files
Files=[]
for file in glob.glob('{0}/*/'.format(path), recursive = True):
    if 'Meta' in file :
        pass
    else:
        Files.append(file)


# In[162]:


#copying all csv files with the same indicator to one sub directory


# In[163]:


for subdir in Files:
    file_list=os.listdir(subdir)
    for i in  range(len(file_list)):
        ext=os.path.splitext(file_list[i])[-1]
        if ext== '.csv':
            file_path=os.path.join(path,subdir,"{0}".format(file_list[i].split('\\')[0].split(".json")[0].split('Data_')[-1].split('_page')[0]))
            file_name=os.path.splitext(file_list[i])[0].split('_')[1]
            indicator=os.path.splitext(file_list[i])[0].split('_')[1]
            country=os.path.splitext(file_path)[0].split("\\")[-1].split('_')[0]
            target_folder=os.path.join('{0}\Master_Data_Indicator_{1}'.format(path,indicator))
            target_folder_name=os.path.splitext(target_folder)[0].split('\\')[-1].split('Master_Data_Indicator_')[-1]+os.path.splitext(target_folder)[-1]

            full_path_to_copied_path=os.path.join(target_folder,"{0}_{1}_{2}".format(country,file_name,'.csv'))
            if file_name==target_folder_name:
                try:
                    shutil.copy(file_path,full_path_to_copied_path)
                except :
                    pass
                    


# In[164]:


#merge several csvs in each of the Master folders
Excel_files=[]
for subdir in Files:
    if 'Master'  in subdir:
        Excel_files.append(subdir)
        
    


# In[165]:


Excel_files


# In[166]:


#reading and concatenating  csv files for same indicator across different countries into one master file
for sub in Excel_files:
    all_files=os.listdir(sub)
    df = pd.concat((pd.read_csv(os.path.join(path,sub,f)) for f in all_files), ignore_index=True,axis=0)
    df.drop(columns='Unnamed: 0',axis='Columns',inplace=True)
    df.to_csv(os.path.join(path,sub,"{0}.csv".format(os.path.basename(os.path.dirname(sub)))))
    


# <b>EXTRA DATA</b>

# In[167]:


#importing request lib
import requests as r


# In[168]:


#target dataset code on eurostat
dataset_code='SDG_04_40'


# In[169]:


SDG=r.get('https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{0}?format=json&compressed=true'.format(dataset_code))


# In[170]:


from urllib.request import urlopen
from io import BytesIO
import gzip
url = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{0}?format=TSV&compressed=true'.format(dataset_code)
save_as='{0}/Extra_Data/{1}.csv'.format(path,dataset_code)

# Download from URL
with urlopen(url) as file:
    content= file.read()
    buff = BytesIO(content)
    f = gzip.GzipFile(fileobj=buff)
    #decode bytes to text
    htmls = f.read().decode()

    with open(save_as, 'w') as download:
        download.write(htmls) 
download.close()


# Extra data

# In[171]:


# Reading downloaded csv
df=pd.read_csv('{0}/Extra_Data/{1}.csv'.format(path,dataset_code))


# In[172]:


df.columns


# In[173]:


df[df.columns[4]].str.split(r"\t",expand=True)


# In[175]:


#dynamically retrieving mishmashed and tab separated columns   
number_of_columns=len([i for i in df[df.columns[4]].str.split(r"\t",expand=True).head(0).columns])


# In[176]:


[i for i in range(number_of_columns)]


# In[177]:


df[[i for i in range(number_of_columns)]]= df[df.columns[4]].str.split('\t',expand=True)


# In[178]:


df


# In[179]:


keys=[key for key in  range(1,number_of_columns)]
years=[value for value in  range(2000,20800,3)]


# In[180]:


keys


# In[181]:


columns=dict(zip(keys,years))
     


# In[182]:


columns


# In[183]:


df.columns


# In[184]:


try:
    df.rename(columns=columns, errors="raise",inplace=True)
    df.rename(columns={0:'Geo'}, errors="raise",inplace=True)
except:
    pass
df.drop(columns= df.columns[4],inplace=True)


# In[185]:


df.to_csv('{0}/Extra_Data/{1}.csv'.format(path,dataset_code),index=False)


# <b>Download Central Asia Data on <b>UN eGov</b></b>

# In[186]:


euro_indices=['E-Government Rank',
'E-Government Index',
'E-Participation Index',
'E-Government Online Service Index',
'E-Government Human Capital Index',
'E-Government Telecommunication Infrastructure Index']


# In[187]:


# import subprocess
#other imports
from selenium import webdriver
options = webdriver.ChromeOptions()
options.add_argument('--no-first-run --no-service-autorun --password-store=basic')
options.add_argument(f'--disable-gpu')
options.add_argument(f'--no-sandbox')
from selenium.webdriver.chrome.service import Service
s = Service(r"C:\Users\HP\Downloads\chromedriver_win32 (2)\chromedriver.exe")
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# In[188]:


#Making a directory to store E-Gov indicators
os.makedirs('{0}/Extra_Data/E-Government'.format(path))
#change default directory for chrome to the current indicator
#     options.add_argument("--start-maximized")
print('download.default_directory:{0}\Extra_Data\E-Government'.format(path))
prefs = {"profile.default_content_settings.popups": 0,
                 "download.default_directory":'{0}\Extra_Data\E-Government'.format(path), # IMPORTANT - ENDING SLASH V IMPORTANT
                 "directory_upgrade": True}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(service=s, options=options)
driver.get('https://publicadministration.un.org/egovkb/en-us/Data-Center') 
for i in range(1,20,1):
    try:
        Webelement=driver.find_element(By.CSS_SELECTOR,'#dnn_ctr633_ModuleContent > div > div > div.form > div:nth-child(1) > div > button')
        Webelement.click()
        element = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/ul/li[{0}]/label/span".format(i))))
        element.click()
        print('Downloaded File Number -*-*-> {0}'.format(i))
        update = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/div[3]/div/section[1]/div/div/div/div[1]/div/div/div/div[1]/div[7]/div/input')))
        update.click()  
        csv_download= WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/div[3]/div/section[1]/div/div/div/div[1]/div/div/div/div[3]/div[1]/div/div[2]/a')))
        csv_download.click()
    except:
        print('Exiting')
        driver.quit()


# In[189]:


driver.quit()


# <b>Fetching World Bank WGI (World Governance Index)</b>

# In[214]:


import time
#check if directory already exists if yes ,delete otherwise contruct it
if os.path.exists('{0}/Extra_Data/WB WGI'.format(path)):
    shutil.rmtree('{0}/Extra_Data/WB WGI'.format(path), ignore_errors=True)
else:
    print('creating directory')
    os.makedirs('{0}/Extra_Data/WB WGI'.format(path))
#change default directory for chrome to the current indicator
#     options.add_argument("--start-maximized")
print('download.default_directory:{0}\Extra_Data\WB WGI'.format(path))
prefs = {"profile.default_content_settings.popups": 0,
                 "download.default_directory":'{0}\Extra_Data\WB WGI'.format(path),
                 "directory_upgrade": True}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(service=s, options=options)
driver.get('https://info.worldbank.org/governance/wgi') 
#get file download
button=WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div[1]/div[1]/div/ul[2]/li[5]/a')))

button.click()
#close alert window
driver.switch_to.alert.accept()
#wait for 5 seconds for file to download
time.sleep(5)
print('Downloaded File Number -*-*->')
driver.quit()


# In[ ]:




