import requests
          
collection_id = 189          
url = "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata".format(collection_id)
# url = "https://api-production.data.gov.sg/v2/public/api/datasets/{dataset_id}/list-rows".format(collection_id)
# url = "https://data.gov.sg/collections/189/view"
        
response = requests.get(url)
payload = response.json()
dsids = payload['data']['collectionMetadata']['childDatasets']
# print(payload['data']['collectionMetadata']['childDatasets'])

for dsid in dsids:
    metadata_url = "https://api-production.data.gov.sg/v2/public/api/datasets/{}/metadata".format(dsid)
    metadata_response = requests.get(metadata_url)
    metadata_payload = metadata_response.json()
    dataset_name = metadata_payload['data']['name']
    print(f"Dataset ID: {dsid}, Name: {dataset_name}")
