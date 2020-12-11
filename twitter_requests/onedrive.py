import os
import requests
import json
import msal
import calendar
from datetime import datetime
import quickxorhash
import base64

#Configuration
CLIENT_ID = ''
TENANT_ID = ''
AUTHORITY_URL = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)
RESOURCE_URL = 'https://graph.microsoft.com/'
API_VERSION = 'v1.0'
SCOPES = ['Sites.ReadWrite.All','Files.ReadWrite.All'] 

#Check if the actual access token is expired
#If access token is expired get new access token with refresh token, then save new access token and refresh token
#Else use actual access token
def get_access_token(CLIEND_ID,AUTHORITY_URL,SCOPES):
    d = datetime.utcnow()
    t = calendar.timegm(d.utctimetuple())
    with open('access_token','r') as f:
         access_token = json.load(f)

    if t >= int(access_token['expires_in']):
       print('Refreshing token')
       app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY_URL)
       d = datetime.utcnow()
       t = calendar.timegm(d.utctimetuple())
       with open('refresh_token','r') as f:
          refresh_token = json.load(f)
       token = app.acquire_token_by_refresh_token(refresh_token['refresh_token'],scopes=SCOPES)
       with open('refresh_token','w+') as f:
          json.dump({'refresh_token':token['refresh_token']},f)
       with open('access_token','w+') as f:
          json.dump({'expires_in':int(token['expires_in'])+t,'access_token':token['access_token']},f)
       token = token['access_token']
    else:
       print('Using cached access_token')
       token = access_token['access_token']
    return token

token = get_access_token(CLIENT_ID,AUTHORITY_URL,SCOPES)
headers = {'Authorization': 'Bearer {}'.format(token)}
onedrive_destination = '{}/{}/me/drive/root:/emc-backups'.format(RESOURCE_URL,API_VERSION) #DEFAULT FOLDER WHERE STORE BACKUPS
local_folder = r"/datadrive/disk1/pcassotti/dump/emotionalcity"

#Compute hash for all the files inside the folder_in with quickxorhash
#Compare the hash in the remote folder with the hash of the files in the remote onedrive folder_out
#Files are coupled by the name
def check_hash(folder_in,folder_out,headers):
    hashes = {}
    for root, dirs, files in os.walk(folder_in):
        for file_name in files:
            r = requests.get('%s/%s'%(folder_out,file_name), headers=headers)
            cloud_hash = json.loads(r.content.decode())['file']['hashes']['quickXorHash']
            h = quickxorhash.quickxorhash() 
            h.update(open('%s/%s'%(folder_in,file_name),'rb').read())
            code=base64.b64encode(h.digest()).decode()
            hashes[file_name] = cloud_hash==code
    return hashes


#List files and folders inside the folder
#File fields are : file name, date of creation and size
#Folder fields are : folder name, date of creation, size and number of files/folders inside
def list_files_folders(folder,headers):
    files=[]
    folders=[]
    r = json.loads(requests.get('%s:/children'%folder, headers=headers).content.decode())
    for item in r["value"]:
        if 'folder' in item:
           folders.append((item['name'],item['createdDateTime'],item['size'],item['folder']['childCount']))
        else:
           files.append((item['name'],item['createdDateTime'],item['size']))
    return files,folders


def download(folder,headers):
    files=[]
    folders=[]
    r = json.loads(requests.get('%s:/children'%folder, headers=headers).content.decode())
    for item in r["value"]:
        url = requests.get("{}/{}/me/drive/items/{}?select=@microsoft.graph.downloadUrl".format(RESOURCE_URL,API_VERSION,item["id"]),headers=headers).content.decode()
        print(item['id'])
        files.append((item['name'],url))
    return files,folders


#Upload all files inside local folder_in into the remote onedrive folder folder_out
def upload(folder_in,folder_out,headers):
    #Looping through the files inside the source directory
    for root, dirs, files in os.walk(folder_in):
        for file_name in files:
            file_path = os.path.join(root,file_name)
            file_size = os.stat(file_path).st_size
            file_data = open(file_path, 'rb')
    
            if file_size < 4100000: 
               #Perform is simple upload to the API
               r = requests.put(folder_out+"/"+file_name+":/content", data=file_data, headers=headers)

            else:
               #Creating an upload session
               upload_session = requests.post(folder_out+"/"+file_name+":/createUploadSession", headers=headers).json()
            
               with open(file_path, 'rb') as f:
                    total_file_size = os.path.getsize(file_path)
                    chunk_size = 327680
                    chunk_number = total_file_size//chunk_size
                    chunk_leftover = total_file_size - chunk_size * chunk_number
                    i = 0
                    while True:
                       chunk_data = f.read(chunk_size)
                       start_index = i*chunk_size
                       end_index = start_index + chunk_size
                       #If end of file, break
                       if not chunk_data:
                          break
                       if i == chunk_number:
                          end_index = start_index + chunk_leftover
                       #Setting the header with the appropriate chunk data location in the file
                       upload_headers = {'Content-Length':'{}'.format(chunk_size),'Content-Range':'bytes {}-{}/{}'.format(start_index, end_index-1, total_file_size)}
                       #Upload one chunk at a time
                       chunk_data_upload = requests.put(upload_session['uploadUrl'], data=chunk_data, headers=upload_headers)
                       print(chunk_data_upload)
                       print(chunk_data_upload.json())
                       i = i + 1
                    
            file_data.close()

#upload(local_folder,'%s/2020/12/9'%onedrive_destination,headers)
print(download('%s/2020/12/9'%onedrive_destination,headers))
