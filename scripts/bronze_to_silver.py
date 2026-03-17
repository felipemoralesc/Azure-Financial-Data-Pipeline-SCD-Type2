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

        # ==========================================
        # 1. LEER TODOS LOS ARCHIVOS DE BRONZE
        # ==========================================

        print("🔎 Buscando archivos en 01-bronze...")

        bronze_blobs = list(container_client.list_blobs(name_starts_with="01-bronze/"))

        if not bronze_blobs:
            raise Exception("No se encontraron archivos en Bronze")

        # Ordenar por nombre (fecha en el filename)
        bronze_blobs_sorted = sorted(bronze_blobs, key=lambda x: x.name)

        print(f"📂 Total archivos encontrados: {len(bronze_blobs_sorted)}")

        # ==========================================
        # 2. LEER SILVER EXISTENTE
        # ==========================================

        silver_blob_name = "02-silver/stock_data.parquet"
        silver_blob = container_client.get_blob_client(silver_blob_name)

        try:
            print("📥 Leyendo Silver existente...")

            existing_data = silver_blob.download_blob().readall()
            df_existing = pd.read_parquet(BytesIO(existing_data))

            print(f"📊 Registros existentes: {len(df_existing)}")

        except Exception:
            print("⚠️ No existe Silver previo. Se creará uno nuevo.")
            df_existing = pd.DataFrame()

        # ==========================================
        # 3. PROCESAR TODOS LOS ARCHIVOS
        # ==========================================

        all_new_data = []

        for blob in bronze_blobs_sorted:

            print(f"📂 Procesando: {blob.name}")

            blob_client = container_client.get_blob_client(blob.name)

            json_bytes = blob_client.download_blob().readall()
            data_json = json.loads(json_bytes)

            df = pd.DataFrame(data_json)

            # Transformaciones
            df["date"] = pd.to_datetime(df["date"])
            df["price"] = pd.to_numeric(df["4. close"], errors="coerce")
            df["volume"] = pd.to_numeric(df["5. volume"], errors="coerce")

            df = df[["symbol", "date", "price", "volume"]]

            df["processed_at"] = datetime.utcnow()

            # Data Quality
            df = df[df["price"] > 0]
            df = df[df["volume"] >= 0]
            df = df.dropna(subset=["symbol", "price", "volume", "date"])
            df = df.drop_duplicates()

            all_new_data.append(df)

        # Unir todos los nuevos
        df_new = pd.concat(all_new_data, ignore_index=True)

        print(f"📊 Total registros nuevos: {len(df_new)}")

        # ==========================================
        # 4. UNIÓN CON HISTÓRICO
        # ==========================================

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)

        print(f"📊 Total combinado: {len(df_combined)}")

        # ==========================================
        # 5. DEDUPLICACIÓN
        # ==========================================

        df_combined = df_combined.sort_values("processed_at", ascending=False)

        df_final = df_combined.drop_duplicates(
            subset=["symbol", "date"],
            keep="first"
        )

        print(f"✅ Total final sin duplicados: {len(df_final)}")

        # ==========================================
        # 6. GUARDAR EN SILVER
        # ==========================================

        parquet_buffer = BytesIO()
        df_final.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        parquet_buffer.seek(0)

        silver_blob.upload_blob(parquet_buffer, overwrite=True)

        print("🚀 ¡Capa Silver actualizada (MULTI-ARCHIVO + INCREMENTAL)!")

    except Exception as e:
        print(f"❌ Error en la transformación: {e}")


if __name__ == "__main__":
    transform_bronze_to_silver()
