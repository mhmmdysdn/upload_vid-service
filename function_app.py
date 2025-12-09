import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient, PartitionKey
import uuid
import logging
import json
import datetime
import os

app = func.FunctionApp()

# Konfigurasi connection string Blob Storage
# (Disarankan pindahkan ke local.settings.json nanti untuk keamanan)
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVqRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"

# KONFIGURASI COSMOS DB
COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhNLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"

def get_cosmos_client():
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container
    
@app.route(route="uploadVideo", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def upload_video(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Upload video request diterima...")

        # 1. Cek User ID
        user_id = req.headers.get('x-user-id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "Unauthorized. Header 'x-user-id' wajib disertakan."}),
                status_code=401,
                mimetype="application/json"
            )

        # 2. Ambil USERNAME & CAPTION dari Form Data
        username = req.form.get('username')
        if not username:
            username = "Unknown User"

        # --- TAMBAHAN CAPTION ---
        caption = req.form.get('caption')
        if not caption:
            caption = "" 
        # ------------------------

        # 3. Ambil file video
        file = req.files.get('video')
        if not file:
            return func.HttpResponse(
                json.dumps({"error": "Video file is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # 4. Validasi MIME type
        allowed_types = ["video/mp4", "video/webm", "video/ogg"]
        if file.content_type not in allowed_types:
            return func.HttpResponse(
                json.dumps({"error": f"Tipe file tidak diizinkan: {file.content_type}"}),
                status_code=400,
                mimetype="application/json"
            )

        # 5. Generate nama file unik & Upload Blob
        ext = file.filename.split(".")[-1]
        file_id = str(uuid.uuid4())
        file_name = f"{file_id}.{ext}"

        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)

        container_client.upload_blob(
            name=file_name,
            data=file.stream,
            overwrite=False
        )
        
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]
        video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{file_name}"

        # 6. Simpan Metadata ke Cosmos DB
        video_metadata = {
            "id": file_id,
            "userId": user_id, 
            "username": username,
            "caption": caption,   # <--- JANGAN LUPA INI
            "fileName": file_name,
            "originalFileName": file.filename,
            "contentType": file.content_type,
            "blobUrl": video_url,
            "uploadTime": datetime.datetime.utcnow().isoformat(),
            "status": "uploaded",
            "likes": 0
        }

        cosmos_container = get_cosmos_client()
        cosmos_container.create_item(body=video_metadata)
        
        logging.info(f"Metadata disimpan. ID: {file_id}, Caption: {caption}")

        return func.HttpResponse(
            body=json.dumps({
                "message": "Upload success", 
                "id": file_id, 
                "url": video_url,
                "uploader": username 
            }),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Error saat upload: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal Server Error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )