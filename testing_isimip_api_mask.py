#Libraries
import matplotlib.pyplot as plt
import requests
import xarray as xr
import json
from pathlib import Path
import time
import zipfile

#Accessing FishMIP data
response = requests.get('https://data.isimip.org/api/v1/datasets', params={
    'simulation_round': 'ISIMIP3b',
    'product': 'OutputData',
    'climate_forcing': 'ipsl-cm6a-lr',
    'climate_scenario': 'historical',
    'sector': 'marine-fishery_global',
    'model': 'apecosm',
    'variable': 'tcb'
})

#Connecting to API
response.raise_for_status()
response_data = response.json()
dataset = response_data['results'][0]
#This dataset contains one file only
paths = [file['path'] for file in dataset['files']]


#Cut out area and create mask based 
operations = [
    #first, cut-out around Prydz Bay
    {
        'operation': 'cutout_bbox',
        'bbox': [
             60,  # west
             90,  # east
            -70,  # south
            -55   # north
        ]
    },
    # mask the file using the created mask
    {
        'operation': 'mask_mask',
        'mask': 'test.nc',
	#even if 'var' is commented out, the request does not work
	'var': 'region'
    }
]

#Connect to API again to create mask
response = requests.post('https://files.isimip.org/api/v2', files={
    'data': json.dumps({
        'paths': paths,
        'operations': operations
    }),
    'test.nc': Path('test.nc').read_bytes(),  # needs to be the same as in the create_mask operation
})
response.raise_for_status()
job = response.json() #Status is 'failed' when using a mask not produced by/modified from API in operations

#Send request to API
while True:
  job = requests.get(job['job_url']).json()
  print(job['status'], job['meta'])

  time.sleep(10)
  if job['status'] not in ['queued', 'started']:
      break
      
#Download request
zip_path = Path(job['file_name'])
with requests.get(job['file_url'], stream=True) as response:
    with zip_path.open('wb') as fp:
        for chunk in response.iter_content(chunk_size=8192):
            fp.write(chunk)

#Unzip data
out_path = zip_path.with_suffix('')
with zipfile.ZipFile(zip_path, 'r') as zf:
    zf.extractall(out_path)

