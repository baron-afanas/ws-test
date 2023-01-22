from lib import FileHandler
from lib import GoogleCloudStorage
import functions_framework
import logging
from lib import BqUploader
from datetime import datetime
import os


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def main(cloud_event):
    TIMESTAMP = int(datetime.now().timestamp())
    PROJECT = os.environ.get('project')
    DATASET= os.environ.get('dataset')
    TABLE = os.environ.get('table')
    TMP_FILE = "/tmp/temp.json"
    event_data = cloud_event.data

    if not event_data["name"].lower().endswith('json'):
        print({
            "worker": "data-loader",
            "timestamp": TIMESTAMP,
            "filename":event_data["name"], 
            "status":"error", 
            "info": "not a valid file type"
        })
        return "not a valid file type"

    storage = GoogleCloudStorage(PROJECT)

    file = storage.download(event_data["bucket"],event_data["name"],TMP_FILE)
    if file.success is not True:
        print({
            "worker": "data-loader",
            "timestamp": TIMESTAMP,
            "filename":event_data["name"], 
            "status":"error", 
            "info": file.message, 
            "valid_entries":0, 
            "error_entries":0
        })
        return "error loading the file"
    
    handler = FileHandler()
    load = handler.file_loader(TMP_FILE)
    if load.success is not True:
        print({
            "worker": "data-loader",
            "timestamp": TIMESTAMP,
            "filename":event_data["name"], 
            "status":"error", 
            "info": load.message,
            "valid_entries":0, 
            "error_entries":0
        })
        return "file not loaded"
    
    parse = handler.file_parser()
    if not parse.success:
        print({
            "worker": "data-loader",
            "timestamp": TIMESTAMP,
            "filename":event_data["name"], 
            "status":"error", 
            "info": parse.message, 
            "valid_entries":0, 
            "error_entries":0
        })
        return

    uploader = BqUploader(PROJECT)
    upload_data = uploader.upload_dataframe(DATASET,TABLE,handler.data)
    if upload_data is not True:
        print({
            "worker": "data-loader",
            "timestamp": TIMESTAMP,
            "filename":event_data["name"], 
            "status":"error", 
            "info": upload_data.message, 
            "valid_entries":len(handler.data), 
            "error_entries":len(handler.errors)
        })
        return "error uploading to BQ"
    print({
        "worker": "data-loader",
        "timestamp": TIMESTAMP,
        "filename":event_data["name"], 
        "status":"success", 
        "info": upload_data.message, 
        "valid_entries":len(handler.data), 
        "error_entries":len(handler.errors)
    })
    return "success"
