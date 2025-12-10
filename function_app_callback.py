import azure.functions as func
from azure.cosmos import CosmosClient
import json
import logging

app = func.FunctionApp()

# --- Konfigurasi ---
# Dapatkan koneksi Cosmos (untuk update metadata akhir)
COSMOS_DB_ENDPOINT = "https://tiktok.documents.azure.com:443/"
COSMOS_DB_KEY = "OYNwhYosf6V4QaDxBIjgm2FkZXw53W0pErxYJyMKVZEGhXsdYhLeOWvvq77DiWqpgu0uc4KrzPiACDb3WfdwQ=="
COSMOS_DB_DATABASE_NAME = "VideoMetadataDB"
COSMOS_DB_CONTAINER_NAME = "Videos"
# --------------------

def get_cosmos_container():
    client = CosmosClient(COSMOS_DB_ENDPOINT, COSMOS_DB_KEY)
    database = client.get_database_client(COSMOS_DB_DATABASE_NAME)
    container = database.get_container_client(COSMOS_DB_CONTAINER_NAME)
    return container

@app.route(route="transcodeCallback", auth_level=func.AuthLevel.FUNCTION, methods=["POST"])
def transcoding_complete_callback(req: func.HttpRequest) -> func.HttpResponse:
    """
    Dipicu oleh Webhook/Event Grid dari Azure Media Services atau layanan Transcoding.
    """
    logging.info("Function D (Callback): Notifikasi Transcoding Selesai diterima.")
    
    try:
        req_body = req.get_json()
        
        # --- Asumsi Struktur Pesan dari Layanan Transcoding ---
        video_id = req_body.get('videoId')
        status = req_body.get('jobStatus')
        processed_urls = req_body.get('processedUrls') # Daftar URL ABR
        # ---------------------------------------------------
        
        if status != 'Completed' or not video_id:
             logging.error(f"Transcoding gagal atau ID tidak ada untuk ID: {video_id}")
             return func.HttpResponse("Job Gagal atau Status Tidak Sesuai", status_code=202)

        cosmos_container = get_cosmos_container()
        
        # 1. Ambil item saat ini
        item = cosmos_container.read_item(item=video_id, partition_key=video_id) 

        # 2. Update status dan URL
        item['status'] = 'ready_for_playback'
        item['processedUrls'] = processed_urls
        item['processingCompletionTime'] = datetime.datetime.utcnow().isoformat()

        # 3. Ganti item di Cosmos DB
        cosmos_container.replace_item(item=item, body=item)
        
        logging.info(f"SUCCESS: Metadata akhir berhasil diperbarui untuk ID: {video_id}. Video siap ditonton.")

        return func.HttpResponse(json.dumps({"message": "Metadata berhasil diperbarui"}), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error pada Function D (Callback): {e}")
        return func.HttpResponse(json.dumps({"error": f"Internal Server Error: {str(e)}"}), status_code=500, mimetype="application/json")
