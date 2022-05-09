import os
from google.cloud import storage
import jsonpickle
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/home/io/gcloud_home/gcp-dataflow-test/pubsub-to-bigq/data-flow-sa.json"

client = storage.Client()
for blob in client.list_blobs('dump-bucket-3', prefix='model'):
    file_name = blob.name.split('/')[-1]
    print(file_name)
    blob.download_to_filename(file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        model = json.load(f)
        print(jsonpickle.encode(model))
