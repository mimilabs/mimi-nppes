# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Overview
# MAGIC
# MAGIC This script downloads three types of Zip files from the CMS website: https://download.cms.gov/nppes/NPI_Files.html
# MAGIC
# MAGIC - Full Replacement Monthly NPI File: https://download.cms.gov/nppes/NPPES_Data_Dissemination_{month}_{year}.zip
# MAGIC
# MAGIC - Full Replacement Monthly NPI Deactivation File: https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_{mmddyy}.zip
# MAGIC - Weekly Incremental NPI Files: https://download.cms.gov/nppes/NPPES_Data_Dissemination_{mmddyy}_{mmddyy}_Weekly.zip
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect files to download

# COMMAND ----------

!pip install requests beautifulsoup4 tqdm python-dateutil

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

url = "https://download.cms.gov/nppes"
page = "NPI_Files.html"
volumepath_root = "/Volumes/mimi_ws_1/nppes/src"
volumepath_zip = f"{volumepath_root}/zipfiles"
retrieval_range = 48 # in months

# COMMAND ----------

# Fetch the webpage
response = requests.get(f"{url}/{page}")
response.raise_for_status()  # This will raise an error if the fetch fails

# Parse the HTML content
soup = BeautifulSoup(response.text, 'html.parser')

# Find all <a> tags, then filter out those without a .zip in their href attribute
files_to_download = [Path(a['href']).name 
                        for a in soup.find_all('a', href=True) 
                        if a['href'].endswith('.zip')]

# COMMAND ----------

ref_monthyear = datetime.datetime.now()
for mon_diff in range(1, retrieval_range): 
    monthyear = (ref_monthyear - relativedelta(months=mon_diff)).strftime('%B_%Y')
    files_to_download.append(f"NPPES_Data_Dissemination_{monthyear}.zip")

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}/{filename}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# Display the zip file links
for filename in files_to_download:
    # Check if the file exists
    if Path(f"{volumepath_zip}/{filename}").exists():
        # print(f"{filename} exists, skipping...")
        continue
    else:
        print(f"{filename} downloading...")
        download_file(url,filename, volumepath_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("*.zip")
                    if x.stem[-7:] != "_Weekly" and x.stem[:17] != "NPPES_Deactivated"]

# COMMAND ----------

for file_downloaded in files_downloaded:
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            ext = ""
            if  member.lower()[-14:] == "fileheader.csv":
                ext = "_header"
            for filetype in ["npidata", "endpoint", "othername", "pl"]:
                if (member[:(len(filetype)+1)] == f"{filetype}_" and
                    not Path(f"{volumepath_root}/{filetype}{ext}/{member}").exists()):
                    print(f"Extracting {member}...")
                    zip_ref.extract(member, path=f"{volumepath_root}/{filetype}{ext}")

# COMMAND ----------


