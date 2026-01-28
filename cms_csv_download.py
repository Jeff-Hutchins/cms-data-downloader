import pandas as pd
import requests
import re
import json
import os
import datetime
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from logging.handlers import RotatingFileHandler

'''
A simple Python script that downloads the latest CMS Hospital datasets 
from data.cms.gov, processes them (snake_case column names), 
and saves them locally. Only downloads files modified since the last run.
'''

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('runs.log'),
        logging.StreamHandler()
    ]
)

# Rotating file handler that prevents file from being too big
handler = RotatingFileHandler(
    'runs.log',
    maxBytes=10*1024*1024,
    backupCount=5    
)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(handler)

logger = logging.getLogger(__name__)


# CSV download function to be used in ThreadPoolExecutor
def create_file_per_dataset(dataset, last_run, downloads):
    
    title = dataset['title']
    
    distribution = dataset.get('distribution', [])
    
    # Find download URLs for all datasets
    for i in distribution:
        if i.get('mediaType') == 'text/csv':
            csv_url = i.get('downloadURL')
        else:
            csv_url = None
            logger.error(f"No URL: {title}")
            return
    
    # Find csv file name in csv_url and save in variable file_name
    for i in csv_url.split('/'):
        if ".csv" in i:
            file_name = i
            
    dataset_lmd_str = dataset['modified']
    dataset_lmd = datetime.date.fromisoformat(dataset_lmd_str)
        
    identifier = dataset['identifier']
    
    if last_run and dataset_lmd <= last_run:
        logger.info(f"Skipping {title} (not modified since {last_run}).")
        return
            
    temp_file = file_name + '.temp'
            
    try:
        response = requests.get(csv_url, stream=True)

        with open(os.path.join(downloads, temp_file), 'wb') as f:
            for chunk in response.iter_content(chunk_size=100):
                if chunk:
                    f.write(chunk)
                
        df = pd.read_csv(os.path.join(downloads, temp_file), dtype=str)
                        
        df.columns = (
            df.columns.str.lower()
            .str.strip()          
            .str.replace(" ", "_")       
            .str.strip("_") 
        )
        
        
        df.to_csv(os.path.join(downloads, file_name), index=False)        
        
        os.remove(os.path.join(downloads, temp_file))
                
        logger.info(f"Updated dataset for file = {file_name}")
        
    except Exception as e:
        logger.info(f"Found {len(hospital_datasets)} datasets")
        logger.info(f"Error processing {file_name}: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        
downloads = 'cms_hospital_data'
last_run_file = 'last_run.json'
os.makedirs(downloads,exist_ok=True)

cms_api = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
response = requests.get(cms_api)
data = response.json()

# Find only datasets with the "theme" of "Hospitals"
hospital_datasets = [dataset for dataset in data if "Hospitals" in dataset.get('theme', [])]

# Load last run date
last_run = None
if os.path.exists(last_run_file):
    with open(last_run_file, 'r') as f:
        data = json.load(f)
        last_run_str = data.get('last_run')
        if last_run_str:
            last_run = datetime.date.fromisoformat(last_run_str)


with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for dataset in hospital_datasets:
        future = executor.submit(create_file_per_dataset, dataset, last_run, downloads)
        futures.append(future)

    for future in as_completed(futures):
        future.result()

current_date = datetime.date.today().isoformat()
with open(last_run_file, 'w') as f:
    json.dump({'last_run': current_date}, f)
print(f"Updated last run date to {current_date}.")