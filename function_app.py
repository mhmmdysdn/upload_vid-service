import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient
from azure.cosmos import CosmosClient
import uuid
import logging
import json
import datetime 
import time

# ----------------------------------------------------------------
# APLIKASI UTAMA AZURE FUNCTION (FUNCTION A, C, D)
# ----------------------------------------------------------------
app = func.FunctionApp()

# ##################################################################
# ###################### KONFIGURASI HARDCODE ######################
# ##################################################################
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVqRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"
QUEUE_CONN_STRING = BLOB_CONN_STRING
# QUEUE_NAME_METADATA = "video-metadata-queue" # <- DIHAPUS
QUEUE_NAME_TRANSCODE_JOB = "video-transcode-jobs" 

COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"
# ##################################################################


def get_cosmos_container():
    """Utility function untuk mendapatkan klien Cosmos DB."""
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container

# ================================================================
# FUNCTION A: HTTP Trigger (upload_video) - MODIFIED
# ================================================================
@app.route(route="uploadVideo", methods=["POST"])
def upload_video(req: func.HttpRequest) -> func.HttpResponse:
    """
    Menerima file, mengunggah ke Blob, MENGUBAH METADATA DI COSMOS DB, 
    dan mengirim pesan ke Queue Transcode.
    """
    try:
        logging.info("Function A (Upload): Request diterima.")
        
        user_id = req.headers.get('x-user-id')
        username = req.form.get('username') or "Unknown User"
        caption = req.form.get('caption') or ""
        file = req.files.get('video')
        
        if not file:
            return func.HttpResponse(json.dumps({"error": "Video file is required"}), status_code=400, mimetype="application/json")
        
        allowed_types = ["video/mp4", "video/webm", "video/ogg"]
        if file.content_type not in allowed_types:
            return func.HttpResponse(json.dumps({"error": f"Tipe file tidak diizinkan: {file.content_type}"}), status_code=400, mimetype="application/json")

        # 1. Upload Blob
        ext = file.filename.split(".")[-1]
        file_id = str(uuid.uuid4())
        file_name = f"{file_id}.{ext}"

        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)
        
        # NOTE: Jika container belum ada, Anda mungkin perlu menambahkan container_client.create_container(exists_ok=True)
        container_client.upload_blob(name=file_name, data=file.stream, overwrite=False)
        
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]
        video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"
        
        logging.info(f"Blob berhasil diunggah: {file_name}")

        # 2. Persiapan Metadata dan Penyimpanan Cosmos DB (LOGIKA DARI FUNGSI B)
        video_metadata = {
            "id": file_id,
            "userId": user_id,
            "username": username,
            "caption": caption,
            "fileName": file_name,
            "originalFileName": file.filename,
            "contentType": file.content_type,
            "blobUrl": video_url,
            "uploadTime": datetime.datetime.utcnow().isoformat(),
            "status": "transcoding_queued", # Status awal langsung disetel ke antrean transcode
            "likes": 0
        }

        # Simpan Metadata Awal ke Cosmos DB
        cosmos_container = get_cosmos_container()
        cosmos_container.create_item(body=video_metadata)
        logging.info(f"Metadata awal disimpan di Cosmos DB. ID: {file_id}. Sekarang kirim Job Transcoding.")

        # 3. Kirim Job Transcoding ke Queue (LOGIKA DARI FUNGSI B)
        queue_service = QueueServiceClient.from_connection_string(QUEUE_CONN_STRING)
        queue_client_transcode = queue_service.get_queue_client(QUEUE_NAME_TRANSCODE_JOB)
        
        # *** FIX UNTUK MENGHILANGKAN QUEUENOTFOUND ***
        queue_client_transcode.create_queue(fail_on_exist=False)
        # **********************************************

        transcode_job_data = {
            "videoId": file_id,
            "sourceBlobUrl": video_url,
            "targetContainer": "processed-videos" 
        }
        
        queue_client_transcode.send_message(json.dumps(transcode_job_data))
        logging.info(f"Job Transcoding dikirim untuk ID: {file_id}")

        # 4. Kirim Respons HTTP 200 ke Klien
        return func.HttpResponse(
            body=json.dumps({
                "message": "Upload berhasil. Video sedang diproses di latar belakang.", 
                "id": file_id,
                "url": video_url,
                "uploader": username
            }),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Error pada Function A (upload_video): {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal Server Error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )
        
# ================================================================
# FUNCTION B: Queue Trigger (process_metadata) - DIHILANGKAN
# ================================================================

# ================================================================
# FUNCTION C: Queue Trigger (start_transcoding) - TETAP
# ================================================================
@app.queue_trigger(arg_name="job_msg", 
                   queue_name=QUEUE_NAME_TRANSCODE_JOB,
                   connection="AzureWebJobsStorage") 
def start_transcoding(job_msg: func.QueueMessage):
    logging.info("Function C (Transcoder): Job Transcode diterima.")
    
    try:
        job_data = json.loads(job_msg.get_body().decode('utf-8'))
        video_id = job_data.get('videoId')
        source_url = job_data.get('sourceBlobUrl')
        
        logging.warning(f"Memulai pekerjaan transcoding simulasi untuk ID: {video_id} dari URL: {source_url}")

        time.sleep(10) # Simulasi pemrosesan 10 detik

        logging.info(f"SUCCESS: Transcoding Job selesai (Simulasi) untuk ID: {video_id}")
        
    except Exception as e:
        logging.error(f"FATAL ERROR: Gagal memicu/melakukan Transcoding: {e}")
        raise

# ================================================================
# FUNCTION D: HTTP Trigger (transcoding_complete_callback) - TETAP
# ================================================================
@app.route(route="transcodeCallback", methods=["POST"])
def transcoding_complete_callback(req: func.HttpRequest) -> func.HttpResponse:
    """Dipicu oleh Webhook dari layanan Transcoding untuk update status akhir."""
    logging.info("Function D (Callback): Notifikasi Transcoding Selesai diterima, termasuk Thumbnail.")
    
    try:
        req_body = req.get_json()
        
        video_id = req_body.get('videoId')
        status = req_body.get('jobStatus')
        processed_urls = req_body.get('processedUrls')
        thumbnail_url = req_body.get('thumbnailUrl')
        
        if status != 'Completed' or not video_id:
             logging.error(f"Transcoding gagal atau ID tidak ada untuk ID: {video_id}")
             return func.HttpResponse("Job Gagal atau Status Tidak Sesuai", status_code=202)

        cosmos_container = get_cosmos_container()
        
        # 1. Ambil item saat ini
        item = cosmos_container.read_item(item=video_id, partition_key=video_id) 

        # 2. Update status, URL Video, DAN URL Thumbnail
        item['status'] = 'ready_for_playback'
        item['processedUrls'] = processed_urls 
        item['thumbnailUrl'] = thumbnail_url  
        item['processingCompletionTime'] = datetime.datetime.utcnow().isoformat()

        # 3. Ganti item di Cosmos DB
        cosmos_container.replace_item(item=item, body=item)
        
        logging.info(f"SUCCESS: Metadata akhir (termasuk thumbnail) diperbarui untuk ID: {video_id}.")

        return func.HttpResponse(json.dumps({"message": "Metadata berhasil diperbarui"}), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error pada Function D (Callback): {e}")
        return func.HttpResponse(json.dumps({"error": f"Internal Server Error: {str(e)}"}), status_code=500, mimetype="application/json")
