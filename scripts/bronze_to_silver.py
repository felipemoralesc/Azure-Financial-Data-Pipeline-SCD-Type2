import pandas as pd
import json
import os
import logging
from io import BytesIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

# ==========================================
# CONFIGURACIÓN GENERAL
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(os.path.join(BASE_DIR, ".env"))

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

METADATA_PATH = "metadata/processed_files.json"
SILVER_PATH = "02-silver/stock_data.parquet"

# ==========================================
# CONFIGURACIÓN LOGGING
# ==========================================

log_dir = BASE_DIR / "logs"
os.makedirs(log_dir, exist_ok=True)

log_filename = log_dir / f"bronze_to_silver_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    filename=str(log_filename),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger()

# ==========================================
# PIPELINE
# ==========================================

def transform_bronze_to_silver():

    start_time = datetime.now()
    logger.info("🚀 Inicio del proceso bronze_to_silver")

    try:
        logger.info("☁️ Conectando con Azure Data Lake...")

        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING
        )

        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # ==========================================
        # 1. LEER METADATA
        # ==========================================

        metadata_blob = container_client.get_blob_client(METADATA_PATH)

        try:
            metadata_bytes = metadata_blob.download_blob().readall()
            processed_files = json.loads(metadata_bytes)
            logger.info(f"📘 Archivos ya procesados: {len(processed_files)}")
        except Exception:
            logger.warning("⚠️ No existe metadata. Se inicializa vacío.")
            processed_files = []

        # ==========================================
        # 2. LISTAR ARCHIVOS EN BRONZE
        # ==========================================

        bronze_blobs = list(container_client.list_blobs(name_starts_with="01-bronze/"))

        if not bronze_blobs:
            raise Exception("No se encontraron archivos en Bronze")

        bronze_blobs_sorted = sorted(bronze_blobs, key=lambda x: x.name)

        logger.info(f"📂 Archivos encontrados en Bronze: {len(bronze_blobs_sorted)}")

        # ==========================================
        # 3. FILTRAR SOLO NUEVOS
        # ==========================================

        new_blobs = [b for b in bronze_blobs_sorted if b.name not in processed_files]

        logger.info(f"🆕 Archivos nuevos encontrados: {len(new_blobs)}")

        if not new_blobs:
            logger.info("✅ No hay nuevos archivos para procesar.")
            return

        # ==========================================
        # 4. LEER SILVER EXISTENTE
        # ==========================================

        silver_blob = container_client.get_blob_client(SILVER_PATH)

        try:
            existing_data = silver_blob.download_blob().readall()
            df_existing = pd.read_parquet(BytesIO(existing_data))
            logger.info(f"📊 Registros existentes en Silver: {len(df_existing)}")
        except Exception:
            logger.warning("⚠️ No existe Silver previo.")
            df_existing = pd.DataFrame()

        # ==========================================
        # 5. PROCESAR NUEVOS ARCHIVOS
        # ==========================================

        all_new_data = []

        for blob in new_blobs:

            logger.info(f"📂 Procesando archivo: {blob.name}")

            blob_client = container_client.get_blob_client(blob.name)

            json_bytes = blob_client.download_blob().readall()
            data_json = json.loads(json_bytes)

            df = pd.DataFrame(data_json)

            total_records = len(df)

            # Transformaciones
            df["date"] = pd.to_datetime(df["date"])
            df["price"] = pd.to_numeric(df["4. close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["5. volume"], errors="coerce")

            df = df[["symbol", "date", "price", "volume"]]
            df["processed_at"] = datetime.utcnow()

            # Data Quality
            df_clean = df[
                (df["price"] > 0) &
                (df["volume"] >= 0)
            ]

            df_clean = df_clean.dropna(subset=["symbol", "price", "volume", "date"])
            df_clean = df_clean.drop_duplicates()

            valid_records = len(df_clean)
            invalid_records = total_records - valid_records

            logger.info(f"Total registros: {total_records}")
            logger.info(f"Registros válidos: {valid_records}")
            logger.info(f"Registros descartados: {invalid_records}")

            all_new_data.append(df_clean)

        df_new = pd.concat(all_new_data, ignore_index=True)

        logger.info(f"📊 Registros nuevos procesados: {len(df_new)}")

        # ==========================================
        # 6. UNIÓN + DEDUPLICACIÓN
        # ==========================================

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        df_combined = df_combined.sort_values("processed_at", ascending=False)

        df_final = df_combined.drop_duplicates(
            subset=["symbol", "date"],
            keep="first"
        )

        logger.info(f"✅ Total final sin duplicados: {len(df_final)}")

        # ==========================================
        # 7. GUARDAR SILVER
        # ==========================================

        parquet_buffer = BytesIO()
        df_final.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        parquet_buffer.seek(0)

        silver_blob.upload_blob(parquet_buffer, overwrite=True)

        logger.info("🚀 Silver actualizado correctamente")

        # ==========================================
        # 8. ACTUALIZAR METADATA
        # ==========================================

        processed_files.extend([b.name for b in new_blobs])

        metadata_blob.upload_blob(
            json.dumps(processed_files, indent=2),
            overwrite=True
        )

        logger.info(f"📘 Metadata actualizada con {len(new_blobs)} archivos nuevos")

    except Exception as e:
        logger.error(f"❌ Error en el pipeline: {str(e)}")
        raise

    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"⏱ Tiempo total de ejecución: {duration} segundos")


if __name__ == "__main__":
    transform_bronze_to_silver()
