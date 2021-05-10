import requests
import json
import os
import google.auth.transport.requests
from google.oauth2 import service_account
from google.cloud import secretmanager,bigquery


REMOVABLE_STATES = ['Selling' , 'Trashed', 'Missing', 'Retired', 'Reserved', 'Stolen', 'Escalated to Legal/HR']
SCOPES = ['https://www.googleapis.com/auth/cloud-identity.devices']
BASE_URL = 'https://cloudidentity.googleapis.com/v1/'
# Enter the administrator to call as here.
ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL')
PROJECT_ID = os.environ.get('PROJECT_ID')
SECRET_NAME = os.environ.get('SECRET_NAME') # "sa-key-cloud-identity-devices"
SECRET_VERSION = f"projects/" + PROJECT_ID + "/secrets/" + SECRET_NAME + "/versions/latest"

def getRemovableAssets():
    # Get Serial numbers of devices that need to be removed
    client = bigquery.Client()
    query_job = client.query(
        """
            SELECT  type_fields.serial_number_10000399951, asset_type_id, type_fields.os_10000399956, asset_types.name, assets.created_at, type_fields.product_10000399951, assets.updated_at, assets.type_fields.asset_state_10000399951
            FROM `bi-datawarehousing-qlik.freshservice.assets` as assets
            JOIN `bi-datawarehousing-qlik.freshservice.asset_types` as asset_types ON assets.asset_type_id = asset_types.id
            WHERE 
            assets.updated_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 DAY) AND CURRENT_TIMESTAMP()
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Monitor')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Headset')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'iPads')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Projector')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Hotspot')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Hardware')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Other Devices')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Virtual Machine')
            AND  NOT REGEXP_CONTAINS(asset_types.name, 'Microsoft Surface')
            AND type_fields.serial_number_10000399951	IS NOT NULL
        """
    )

    results = query_job.result()  # Waits for job to complete.
    ids = []

    for row in results:
        id = row[0]
        os = row[2]
        name = row[3]
        product = row[5]
        state = row[7]
        
        if state in REMOVABLE_STATES:
            print(row)
            ids.append(id)

    if not ids:
        print('No new assets found.')
    else:
        print("Assets retrieved for deletion.")
        return ids

def getDeviceNames(serialNumber):
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = """
        SELECT serialNumber, name
        FROM `bi-datawarehousing-qlik.cloud_identity.devices`
        WHERE serialNumber = @serialNumber
        LIMIT 1;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("serialNumber", "STRING", serialNumber),
        ]
    )
    query_job = client.query(query, job_config=job_config)  # Make an API request.

    for row in query_job:
        print("{}: \t{}".format(row.serialNumber, row.name))
        return row.name
    
def create_delegated_credentials(user_email):
  sa_request = {"name": SECRET_VERSION}
  secret_response = sm_client.access_secret_version(sa_request)
  sa_key_secret = secret_response.payload.data.decode("UTF-8")

  credentials = service_account.Credentials.from_service_account_info(
      json.loads(sa_key_secret),
      scopes=SCOPES
  )
  delegated_credentials = credentials.with_subject(user_email)
  return delegated_credentials



######################################################################
# AUTHENTICATE the service account and retrieve an oauth2 access token

def removeDevice(param):


    request = google.auth.transport.requests.Request()
    dc = create_delegated_credentials(ADMIN_EMAIL)
    dc.refresh(request)
    print('Access token: ' + dc.token + '\n')

    ###############################
    header = {
        'authorization': 'Bearer ' + dc.token,
        'Content-Type': 'application/json'
    }

    try:
        print('Request URL: ' + BASE_URL +param)


        results = requests.delete(BASE_URL+param, 
                headers={'authorization': 'Bearer ' + dc.token,
                'Content-Type': 'application/json'
    })

        print(results.content)

    except TypeError as e:
        print (e)





def removeSeparatedAssets(request, payload):
  if __name__ == "__main__":
      removableAssets = getRemovableAssets()

      for id in removableAssets:
          deviceName = getDeviceNames(id)
          removeDevice(deviceName)        
