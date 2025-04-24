import pathlib
import requests

from sam_globus_keepup import SAMWeb_Client, EXPERIMENT

API_URL = 'https://fndca3b.fnal.gov:3880/api/v1/namespace/pnfs/fnal.gov/usr'
FILE = 'data_EventBuilder1_art2_run18437_1_strmBNBZeroBias_20250418T161939.root'

def main():
    result = SAMWeb_Client.getFileAccessUrls(FILE, schema='file')
    if not result:
        print('File not found')
        return

    # extract URL from xroot string
    path = pathlib.PurePath(result[0].split('file://')[1]).relative_to('/pnfs')
    api_url = f'{API_URL}/{path}?locality=True'
    result = requests.get(api_url, verify=False)
    if result.status_code != 200:
        print('Could not get locality')
        return 

    print(result.json()['fileLocality'])



if __name__ == '__main__':
    main()
