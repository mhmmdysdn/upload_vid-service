import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient
import uuid
import logging
import json
import datetime
import os

app = func.FunctionApp()

# --- Shared Configuration (Diulang di sini untuk kelengkapan) ---
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVhRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"
QUEUE_CONN_STRING = BLOB_CONN_STRING
QUEUE_NAME = "video-metadata-queue"
# ----------------------------------------------------------------

@app.route(route="uploadVideo", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def upload_video(req: func.HttpRequest) -> func.HttpResponse:
    """Menerima file, mengunggah ke Blob, dan mengirim pesan ke Queue."""
    try:
        logging.info("Function A (Upload): Request diterima.")

        # 1. Cek User ID & Ambil Data Form
        user_id = req.headers.get('x-user-id')
        if not user_id:
            return func.HttpResponse(json.dumps({"error": "Header 'x-user-id' wajib."}), status_code=401, mimetype="application/json")

        username = req.form.get('username') or "Unknown User"
        caption = req.form.get('caption') or ""
        file = req.files.get('video')
        
        if not file:
            return func.HttpResponse(json.dumps({"error": "Video file is required"}), status_code=400, mimetype="application/json")
        
        # 2. Validasi MIME type
        allowed_types = ["video/mp4", "video/webm", "video/ogg"]
        if file.content_type not in allowed_types:
            return func.HttpResponse(json.dumps({"error": f"Tipe file tidak diizinkan: {file.content_type}"}), status_code=400, mimetype="application/json")

        # 3. Upload Blob
        ext = file.filename.split(".")[-1]
        file_id = str(uuid.uuid4())
        file_name = f"{file_id}.{ext}"

        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)
        
        container_client.upload_blob(name=file_name, data=file.stream, overwrite=False)
        
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]
        video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"
        
        logging.info(f"Blob berhasil diunggah: {file_name}")

        # 4. Kirim Pesan ke Azure Queue Storage (THE EVENT)
        queue_service = QueueServiceClient.from_connection_string(QUEUE_CONN_STRING)
        queue_client = queue_service.get_queue_client(QUEUE_NAME)
        
        queue_message_data = {
            "id": file_id,
            "userId": user_id,
            "username": username,
            "caption": caption,
            "fileName": file_name,
            "originalFileName": file.filename,
            "contentType": file.content_type,
            "blobUrl": video_url,
            "uploadTime": datetime.datetime.utcnow().isoformat(),
            "status": "pending_processing", # Status yang akan diubah oleh Function B
            "likes": 0
        }

        queue_client.send_message(json.dumps(queue_message_data))
        logging.info(f"Pesan metadata dikirim ke Queue. ID: {file_id}")

        # 5. Kirim Respons HTTP 200 ke Klien
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
