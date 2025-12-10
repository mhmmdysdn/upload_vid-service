import azure.functions as func
from azure.storage.blob import BlobServiceClient
# Import exceptions dari azure.cosmos
from azure.cosmos import CosmosClient, PartitionKey, exceptions 
import uuid
import logging
import json
import datetime
import os
import time # Tambahkan untuk simulasi transcoding

# =========================================================
# KONFIGURASI (PENTING: Ganti dengan os.environ di lingkungan produksi)
# =========================================================
# Jika Anda ingin mengikuti praktik terbaik, ubah ini menjadi:
# BLOB_CONN_STRING = os.environ.get("AzureWebJobsStorage") 
# atau di-override di trigger connection="BlobStorageConnection"
BLOB_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=uploadvidservicefunc123;AccountKey=gKt+BNW0iCObVQT7al9DfjKVqRgiCzC78c9zRBWfVg8hrPndGIRibwQl8pkINrrl1+Ts25lxtRFI+ASto3f3YQ==;EndpointSuffix=core.windows.net"
BLOB_CONTAINER_NAME = "videos"
BLOB_TRANSCODED_CONTAINER_NAME = "transcoded-videos" # Kontainer baru untuk hasil transcoding

COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhNLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"

# Asumsikan nama setting koneksi di local.settings.json untuk Trigger
COSMOS_DB_CONN_SETTING = "CosmosDBConnection"
BLOB_STORAGE_CONN_SETTING = "AzureWebJobsStorage" # Default atau nama kustom seperti "BlobStorageConnection"

# =========================================================
# SETUP KLIEN
# =========================================================
app = func.FunctionApp()

def get_cosmos_client():
    """Mengembalikan klien Cosmos DB untuk kontainer Videos"""
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container

# =========================================================
# 1. UPLOAD VIDEO (HTTP Trigger)
# =========================================================
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

        # 2. Ambil data form
        username = req.form.get('username', "Unknown User")
        caption = req.form.get('caption', "")
        file = req.files.get('video')
        
        if not file:
            return func.HttpResponse(
                json.dumps({"error": "Video file is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Simpan nama file asli sebelum validasi, jika diperlukan
        original_file_name = file.filename # <-- Mengambil nama file asli dari pengguna

        # 3. Validasi MIME type
        allowed_types = ["video/mp4", "video/webm", "video/ogg"]
        if file.content_type not in allowed_types:
            return func.HttpResponse(
                json.dumps({"error": f"Tipe file tidak diizinkan: {file.content_type}"}),
                status_code=400,
                mimetype="application/json"
            )

        # 4. Generate nama file unik & Upload Blob (RAW)
        ext = original_file_name.split(".")[-1] # Menggunakan ekstensi dari nama asli
        file_id = str(uuid.uuid4())
        raw_file_name = f"{file_id}.{ext}" # Video mentah di kontainer 'videos'

        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        container_client = blob_service.get_container_client(BLOB_CONTAINER_NAME)

        container_client.upload_blob(
            name=raw_file_name,
            data=file.stream,
            overwrite=False
        )
        
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]
        raw_video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{raw_file_name}"

        # 5. Simpan Metadata Awal ke Cosmos DB
        video_metadata = {
            "id": file_id,
            "userId": user_id, 
            "username": username,
            "caption": caption,
            "originalFileName": original_file_name, # <-- FIELD BARU DITAMBAHKAN
            "fileName": raw_file_name,
            "rawBlobUrl": raw_video_url, # URL video mentah
            "transcodedBlobUrl": None,    # Belum ada
            "uploadTime": datetime.datetime.utcnow().isoformat(),
            "status": "uploaded",         # Status awal
            "likes": 0,
            "duration": 0
        }

        cosmos_container = get_cosmos_client()
        cosmos_container.create_item(body=video_metadata)
        
        logging.info(f"Metadata disimpan. ID: {file_id}, Status: uploaded")

        return func.HttpResponse(
            body=json.dumps({
                "message": "Upload success. Video sedang di-transcoding.", 
                "id": file_id, 
                "original_name": original_file_name, # Tampilkan nama asli di respons
                "raw_url": raw_video_url,
                "status": "processing"
            }),
            mimetype="application/json",
            status_code=202,
        )

    except Exception as e:
        logging.error(f"Error saat upload: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"Internal Server Error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

# =========================================================
# 2. TRANSCODING VIDEO (Blob Trigger)
# =========================================================
# Terpicu setiap ada file baru di kontainer 'videos' (RAW)
@app.blob_trigger(arg_name="myblob", path=BLOB_CONTAINER_NAME + "/{name}", connection=BLOB_STORAGE_CONN_SETTING)
def video_transcoding_trigger(myblob: func.InputStream):
    """Fungsi ini dipicu setelah file video mentah diunggah."""
    
    file_name = myblob.name.split('/')[-1] # Contoh: 12345.mp4
    file_id = file_name.split('.')[0] # Contoh: 12345
    
    logging.info(f"== Transcoding Triggered == ID: {file_id}. Mulai pemrosesan file: {file_name}")

    try:
        cosmos_container = get_cosmos_client()
        
        # 1. Update status metadata di Cosmos DB: processing
        metadata_item = cosmos_container.read_item(item=file_id, partition_key=file_id)
        metadata_item['status'] = 'transcoding_processing'
        cosmos_container.replace_item(item=file_id, body=metadata_item)
        logging.info(f"Status video {file_id} di Cosmos DB diubah menjadi 'transcoding_processing'.")


        # 2. --- SIMULASI LOGIKA TRANSCODING DI SINI ---
        # Di sini Anda akan menggunakan FFMPEG atau Azure Media Services SDK
        
        # Simulasi proses yang memakan waktu
        logging.info(f"Simulasi Transcoding {file_id} (5 detik)...")
        time.sleep(5) 
        
        # Anggap hasil transcoding adalah MP4 dengan resolusi 720p
        transcoded_file_name = f"{file_id}_720p.mp4"
        transcoded_duration = 35 # Simulasi durasi video (detik)
        
        # Simulasi konten hasil transcoding (misal: mengambil 1MB dari video mentah)
        myblob.seek(0)
        simulated_transcoded_data = myblob.read(1024 * 1024) 
        
        # 3. Upload Hasil Transcoding ke kontainer 'transcoded-videos'
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        transcoded_container_client = blob_service.get_container_client(BLOB_TRANSCODED_CONTAINER_NAME)

        transcoded_container_client.upload_blob(
            name=transcoded_file_name,
            data=simulated_transcoded_data,
            overwrite=True
        )

        # 4. Update status dan metadata hasil Transcoding
        storage_account_name = BLOB_CONN_STRING.split("AccountName=")[1].split(";")[0]
        transcoded_video_url = f"https://{storage_account_name}.blob.core.windows.net/{BLOB_TRANSCODED_CONTAINER_NAME}/{transcoded_file_name}"

        # Update metadata di Cosmos DB untuk memicu fungsi 'cosmos_metadata_updater'
        metadata_item['status'] = 'transcoding_complete' # Status berhasil
        metadata_item['transcodedFileName'] = transcoded_file_name
        metadata_item['transcodedBlobUrl'] = transcoded_video_url
        metadata_item['duration'] = transcoded_duration
        
        cosmos_container.replace_item(item=file_id, body=metadata_item)
        logging.info(f"Transcoding {file_id} berhasil. Status diubah menjadi 'transcoding_complete'.")
        
    except exceptions.CosmosResourceNotFoundError:
        logging.error(f"Metadata video {file_id} tidak ditemukan di Cosmos DB.")
    except Exception as e:
        logging.error(f"Error saat Transcoding video {file_id}: {e}")
        # Jika gagal, update status di Cosmos DB ke 'transcoding_failed'
        try:
            cosmos_container = get_cosmos_client()
            failed_item = cosmos_container.read_item(item=file_id, partition_key=file_id)
            failed_item['status'] = 'transcoding_failed'
            cosmos_container.replace_item(item=file_id, body=failed_item)
        except Exception as update_e:
            logging.error(f"Gagal update status gagal untuk {file_id}: {update_e}")


# =========================================================
# 3. METADATA UPDATER (Cosmos DB Trigger)
# =========================================================
# Terpicu setiap ada perubahan (insert atau update) di kontainer 'Videos'
@app.cosmos_db_trigger(
    arg_name="documents",
    database_name=COSMOS_DB_DATABASE_NAME,
    container_name=COSMOS_DB_CONTAINER_NAME,
    connection=COSMOS_DB_CONN_SETTING,
    lease_container_name="leases_videos" 
)
def cosmos_metadata_updater(documents: func.DocumentList):
    """Fungsi ini memastikan metadata video siap ditayangkan."""
    
    if documents:
        for doc in documents:
            doc_dict = doc.to_dict()
            video_id = doc_dict.get('id')
            current_status = doc_dict.get('status')
            
            # Hanya proses yang sudah selesai di-transcoding
            if current_status == 'transcoding_complete':
                logging.info(f"== Metadata Updater Triggered == ID: {video_id}. Finalisasi status.")

                # 1. Lakukan validasi akhir, pembersihan data, atau pembaruan indeks pencarian
                
                # 2. Set status final: ready
                try:
                    cosmos_container = get_cosmos_client()
                    
                    # Ambil lagi untuk memastikan tidak ada konflik
                    final_item = cosmos_container.read_item(item=video_id, partition_key=video_id)
                    
                    final_item['status'] = 'ready' # Status final untuk ditayangkan
                    final_item['isAvailable'] = True
                    final_item['publishDate'] = datetime.datetime.utcnow().isoformat()
                    
                    cosmos_container.replace_item(item=video_id, body=final_item)
                    logging.info(f"Video {video_id} sekarang 'ready' dan siap ditayangkan.")

                except Exception as e:
                    logging.error(f"Gagal finalisasi metadata {video_id}: {e}")
            
            elif current_status == 'uploaded':
                logging.debug(f"Video {video_id} baru diupload. Transcoding akan dipicu oleh Blob Trigger.")
            
            elif current_status == 'ready':
                logging.debug(f"Video {video_id} sudah 'ready'. Mengabaikan pembaruan.")

# =========================================================
# FUNGSI LAIN (Tetap dipertahankan)
# =========================================================

# 4. LIKE VIDEO (POST)
@app.route(route="likeVideo", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def like_video(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        video_id = req_body.get('videoId')
        user_id = req_body.get('userId')

        if not video_id or not user_id:
            return func.HttpResponse(json.dumps({"error": "Missing ID"}), status_code=400)

        container = get_cosmos_client()
        # Untuk mendapatkan hasil yang konsisten, pastikan hanya video 'ready' yang bisa di-like
        item = container.read_item(item=video_id, partition_key=video_id)
        
        if item.get('status') != 'ready':
             return func.HttpResponse(json.dumps({"error": "Video belum siap untuk interaksi"}), status_code=403)


        if 'likedBy' not in item: item['likedBy'] = []
        
        current_likes = item.get('likes', 0)
        
        if user_id in item['likedBy']:
            item['likedBy'].remove(user_id)
            current_likes -= 1
            action = "unliked"
        else:
            item['likedBy'].append(user_id)
            current_likes += 1
            action = "liked"

        if current_likes < 0: current_likes = 0
        item['likes'] = current_likes

        container.replace_item(item=video_id, body=item)

        return func.HttpResponse(
            json.dumps({"message": action, "likes": current_likes}),
            status_code=200
        )
    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse(json.dumps({"error": "Video tidak ditemukan"}), status_code=404)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)


# 5. DELETE VIDEO (POST)
@app.route(route="deleteVideo", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def delete_video(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Request hapus video diterima...")
        
        # 1. Validasi User (Hanya pemilik yang boleh hapus)
        user_id_header = req.headers.get('x-user-id')
        req_body = req.get_json()
        video_id = req_body.get('videoId')

        if not user_id_header or not video_id:
            return func.HttpResponse(json.dumps({"error": "Data tidak lengkap"}), status_code=400)

        container = get_cosmos_client()
        
        # 2. Ambil Data Video
        try:
            item = container.read_item(item=video_id, partition_key=video_id)
        except exceptions.CosmosResourceNotFoundError:
            return func.HttpResponse(json.dumps({"error": "Video tidak ditemukan"}), status_code=404)

        # 3. Cek Kepemilikan (PENTING!)
        video_owner_id = item.get('userId')
        if video_owner_id != user_id_header:
             return func.HttpResponse(json.dumps({"error": "Anda bukan pemilik video ini"}), status_code=403)

        # 4. Hapus File dari Blob Storage (RAW dan TRANSCODED)
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STRING)
        
        # Hapus file RAW
        raw_file_name = item.get('rawFileName')
        if raw_file_name:
            try:
                blob_client_raw = blob_service.get_blob_client(BLOB_CONTAINER_NAME, raw_file_name)
                blob_client_raw.delete_blob(delete_snapshots="include")
            except Exception as e:
                logging.warning(f"Gagal hapus blob RAW (mungkin sudah hilang): {e}")

        # Hapus file TRANSCODED
        transcoded_file_name = item.get('transcodedFileName')
        if transcoded_file_name:
            try:
                blob_client_transcoded = blob_service.get_blob_client(BLOB_TRANSCODED_CONTAINER_NAME, transcoded_file_name)
                blob_client_transcoded.delete_blob(delete_snapshots="include")
            except Exception as e:
                logging.warning(f"Gagal hapus blob TRANSCODED (mungkin sudah hilang): {e}")

        # 5. Hapus Metadata dari Cosmos DB
        container.delete_item(item=video_id, partition_key=video_id)

        return func.HttpResponse(json.dumps({"message": "Video berhasil dihapus"}), status_code=200)

    except Exception as e:
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500)
