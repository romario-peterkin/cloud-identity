import json
import pprint

from six.moves import urllib

import google.auth.transport.requests
from google.oauth2 import service_account


SCOPES = ['https://www.googleapis.com/auth/cloud-identity.devices']
BASE_URL = 'https://cloudidentity.googleapis.com/v1/'

# Change this to the location of the service account key
SA_FILE =  os.environ.get('PATH_TO_FILE')


# Enter the administrator to call as here.
ADMIN_EMAIL = os.environ.get('ADMIN')


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

def createDevice(serialNo, type):


    request = google.auth.transport.requests.Request()
    dc = create_delegated_credentials(ADMIN_EMAIL)
    dc.refresh(request)
    print('Access token: ' + dc.token + '\n')

    ###############################
    # Create the Device
    header = {
        'authorization': 'Bearer ' + dc.token,
        'Content-Type': 'application/json'
    }
    body = {
        'serialNumber': serialNo,  # the serial number of your device.

        # see values in
        # https://cloud.google.com/identity/docs/reference/rest/v1/devices#DeviceType
        'deviceType': type
    }

    serialized_body = json.dumps(body, separators=(',', ':'))

    request_url = BASE_URL + 'devices'
    print('Request URL: ' + request_url)
    print('Request body: ' + serialized_body)

    serialized_body = json.dumps(body, separators=(',', ':'))
    request = urllib.request.Request(request_url, serialized_body.encode('utf-8'), headers=header)
    request.get_method = lambda: 'POST'

    try:
        contents = urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        if e.code == 409:
            print('The request was invalid. Perhaps the device is already present?')
        else:
            print('Unknown error occurred: {}', e.code)

    create_response = json.loads(contents.read())
    inner_response = create_response['response']
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(inner_response)
