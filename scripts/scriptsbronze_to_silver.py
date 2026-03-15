import pandas as pd
import json
import os
from io import BytesIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURACIÓN ---
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(os.path.join(BASE_DIR, ".env"))

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

def transform_bronze_to_silver():

    try:
        print("☁️ Conectando con Azure Data Lake...")

        blob_service_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING
        )

        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        # --- 1. BUSCAR ARCHIVOS EN BRONZE ---
        print("🔎 Buscando archivos en 01-bronze...")

        bronze_blobs = list(container_client.list_blobs(name_starts_with="01-bronze/"))

        if not bronze_blobs:
            raise Exception("No se encontraron archivos en Bronze")

        latest_blob = sorted(bronze_blobs, key=lambda x: x.last_modified)[-1]

        print(f"📂 Archivo encontrado: {latest_blob.name}")

        # --- 2. DESCARGAR JSON DESDE BRONZE ---
        blob_client = container_client.get_blob_client(latest_blob.name)

        json_bytes = blob_client.download_blob().readall()
        data_json = json.loads(json_bytes)

        # --- 3. TRANSFORMACIÓN CON PANDAS ---
        print("⚡ Transformando datos...")

        df = pd.DataFrame(data_json)

        df["date"] = pd.to_datetime(df["date"])
        df["price"] = pd.to_numeric(df["4. close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["5. volume"], errors="coerce")

        df = df[["symbol", "date", "price", "volume"]]

        df["processed_at"] = datetime.utcnow()

        print(f"✅ Transformación exitosa: {len(df)} registros")

        # --- 4. CONVERTIR A PARQUET EN MEMORIA ---
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        # --- 5. SUBIR A SILVER ---
        silver_blob_name = "02-silver/stock_data.parquet"

        silver_blob = container_client.get_blob_client(silver_blob_name)

        parquet_buffer.seek(0)

        silver_blob.upload_blob(parquet_buffer, overwrite=True)

        print("🚀 ¡Capa Silver actualizada!")

    except Exception as e:
        print(f"❌ Error en la transformación: {e}")


if __name__ == "__main__":
    transform_bronze_to_silver()
