import azure.functions as func
from azure.cosmos import CosmosClient
from azure.storage.queue import QueueServiceClient
import json
import logging
import os

app = func.FunctionApp()

# --- Konfigurasi ---
# Dapatkan koneksi Cosmos (untuk penyimpanan metadata awal)
COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"

# Konfigurasi Queue (Function B adalah receiver dari Queue Metadata dan sender ke Queue Transcoding)
QUEUE_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVhRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
QUEUE_NAME_METADATA = "video-metadata-queue"
QUEUE_NAME_TRANSCODE_JOB = "video-transcode-jobs" # <-- Queue BARU
# --------------------

def get_cosmos_container():
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container

@app.queue_trigger(arg_name="msg", 
                   queue_name=QUEUE_NAME_METADATA,
                   connection="AzureWebJobsStorage") 
def process_metadata(msg: func.QueueMessage):
    """
    1. Simpan metadata awal ke Cosmos DB.
    2. Pemicu pekerjaan transcoding dengan mengirim pesan ke Queue Transcode.
    """
    logging.info("Function B (Processor): Pesan Queue Metadata diterima.")
    
    try:
        metadata_json = msg.get_body().decode('utf-8')
        video_metadata = json.loads(metadata_json)
        video_id = video_metadata.get('id', 'N/A')
        
        # 1. Simpan Metadata Awal (Status: Processing)
        video_metadata['status'] = 'transcoding_queued' # Ganti status awal
        cosmos_container = get_cosmos_container()
        cosmos_container.create_item(body=video_metadata)
        logging.info(f"Metadata awal disimpan. ID: {video_id}. Sekarang kirim Job Transcoding.")

        # 2. Kirim Job Transcoding ke Queue Baru (THE NEXT EVENT)
        queue_service = QueueServiceClient.from_connection_string(QUEUE_CONN_STRING)
        queue_client_transcode = queue_service.get_queue_client(QUEUE_NAME_TRANSCODE_JOB)

        transcode_job_data = {
            "videoId": video_id,
            "sourceBlobUrl": video_metadata['blobUrl'],
            "targetContainer": "processed-videos" # Kontainer tempat hasil transcode akan disimpan
        }
        
        queue_client_transcode.send_message(json.dumps(transcode_job_data))
        logging.info(f"Job Transcoding dikirim untuk ID: {video_id}")

    except Exception as e:
        logging.error(f"ERROR: Gagal memproses metadata dan mengirim job transcode: {e}")
        raise
