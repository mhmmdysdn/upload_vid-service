import azure.functions as func
import json
import logging
# import the necessary SDKs here (e.g., Azure Media Services or HttpClient for custom API)
import time # Simulasi waktu pemrosesan

app = func.FunctionApp()

# --- Konfigurasi ---
QUEUE_NAME_TRANSCODE_JOB = "video-transcode-jobs"
# Untuk DEMO: Kita akan mensimulasikan callback/update, 
# tapi dalam produksi, ini akan menjadi Event Grid/Webhook.
# --------------------

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

        # >>> SIMULASI MEMANGGIL AZURE MEDIA SERVICES ATAU FFmpeg DARI AZURE BATCH <<<
        # Di sini, Anda akan memanggil SDK atau API. Contoh:
        # media_client.jobs.begin_create(...)
        time.sleep(10) # Simulasi pemrosesan 10 detik

        # Setelah layanan Transcoding selesai, itu akan memanggil
        # FUNCTION D (Callback/Webhook) dengan hasil akhirnya.
        
        # Karena kita tidak dapat membuat Function D (Webhook) di sini, 
        # anggaplah Transcoding berhasil dan status akan diupdate oleh webhook eksternal.
        logging.info(f"SUCCESS: Transcoding Job selesai (Simulasi) untuk ID: {video_id}")
        
    except Exception as e:
        logging.error(f"FATAL ERROR: Gagal memicu/melakukan Transcoding: {e}")
        # Biarkan ini terjadi agar job bisa dicoba ulang
        raise
