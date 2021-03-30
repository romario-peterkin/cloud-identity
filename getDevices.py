"""Sample script to demonstrate the use of the List method in the Devices API."""
import json
import pprint
import time
from uploadToCloudStorage import upload_blob
from appendTable import appendBigQueryTable
from datetime import datetime, timedelta





from six.moves import urllib

import google.auth.transport.requests
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/cloud-identity.devices']
BASE_URL = 'https://cloudidentity.googleapis.com/v1/'

# Change this to the location of the service account key
SA_FILE = '{PATH_TO_JSON_FILE}'

# Enter the administrator to call as here.
ADMIN_EMAIL = '{ADMIN_ACCOUNT_TO_IMPERSONATE}'

if not SA_FILE:
  print('Please specify the location of the service account key file')
if not ADMIN_EMAIL:
  print('Please specify the email of the administrator to call as')

if not SA_FILE or not ADMIN_EMAIL:
  exit(-1)

def create_delegated_credentials(user_email):
  credentials = service_account.Credentials.from_service_account_file(
      SA_FILE,
      scopes=['https://www.googleapis.com/auth/cloud-identity.devices'])

  delegated_credentials = credentials.with_subject(user_email)


  return delegated_credentials

######################################################################
# AUTHENTICATE the service account and retrieve an oauth2 access token

request = google.auth.transport.requests.Request()
dc = create_delegated_credentials(ADMIN_EMAIL)
dc.refresh(request)
print('Access token: ' + dc.token + '\n')

# Get yesterday's date, for example
yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')


###############################
#initial request for find devices synced from yesterday for the first page
list_url = BASE_URL + 'devices'
auth_header = {'Authorization': 'Bearer ' + dc.token}
content = urllib.request.urlopen(
    urllib.request.Request(list_url+"?pageSize=100&filter=sync:" + yesterday+"..", headers=auth_header)).read()
response = json.loads(content)
pp = pprint.PrettyPrinter(indent=4)

if 'devices' in response:
    print('Listed: ' + str(len(response['devices'])) + ' devices\n')
    nextPageToken = response['nextPageToken']
    master_device_list = response['devices']

    n = 1


    # Loop for pagination
    while nextPageToken:
        print("Retrieving page " + str(n)+"  of devices...")
        req_url = list_url+"?pageSize=100&filter=sync:" + yesterday+"..&pageToken="+nextPageToken
        content = urllib.request.urlopen(
    urllib.request.Request(req_url, headers=auth_header)).read()
        response = json.loads(content)
        try:
            if response['devices']:
                master_device_list += response['devices']
                nextPageToken = response['nextPageToken']
                n += 1
        except:
            break


else:
  print("Something went wrong:")
  print(response)


with open('devices.jsonl', 'w') as outfile:
    for entry in master_device_list:
        json.dump(entry, outfile)
        outfile.write('\n')

with open('devices_f.jsonl', 'w') as outfile:
    for entry in open('devices.jsonl', 'r'):
        y = json.loads(entry)

        # Remove spaces from 'imei' field
        for key in list(y):
            if 'imei' in key:
                y[key] = y[key].replace(' ','')

        
        
        # write to file        
        json.dump(y, outfile)
        outfile.write('\n')


# Declare variables for cloud storage dump
bucket_name = "{BUCKET_NAME}"
source_file_name = "{PATH_TO_SOURCE_FILE}"
destination_blob_name = "{BLOB_NAME}"
dataset = '{BIGQUERY_DATASET_NAME}'
table = '{BIGQUERY_TABLE_NAME}'
uri = "{URI}"




# Upload Json file to Cloud Storage bucket
upload_blob(bucket_name,source_file_name,destination_blob_name)

# Upload Cloud Storage bucket to BigQuery
appendBigQueryTable(dataset,table,uri)

