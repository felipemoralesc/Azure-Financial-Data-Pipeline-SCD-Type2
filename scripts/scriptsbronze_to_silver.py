import pandas as pd
import json
import os
import glob
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN DE RUTAS DINÁMICAS (EL GPS) ---
current_path = Path(__file__).resolve()
# Si el script está en la carpeta 'scripts', sube 2 niveles, si no, sube 1.
if current_path.parent.name == "scripts":
    BASE_DIR = current_path.parent.parent
else:
    BASE_DIR = current_path.parent

# Cargar variables de entorno (.env)
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Variables de Azure
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

def transform_bronze_to_silver():
    try:
        # Definir carpetas de trabajo
        local_data_dir = os.path.join(BASE_DIR, "data")
        temp_parquet = os.path.join(local_data_dir, "stock_data.parquet")
        
        print(f"📍 Buscando archivos en: {local_data_dir}")

        # --- 2. BUSCAR EL JSON MÁS RECIENTE ---
        list_of_files = glob.glob(os.path.join(local_data_dir, "*.json"))
        
        if not list_of_files:
            # Diagnóstico rápido si falla
            contenido = os.listdir(local_data_dir) if os.path.exists(local_data_dir) else "Carpeta no existe"
            raise FileNotFoundError(f"No hay JSONs en {local_data_dir}. Contenido: {contenido}")

        latest_json = max(list_of_files, key=os.path.getctime)
        print(f"📂 Leyendo archivo: {os.path.basename(latest_json)}")

        # --- 3. CARGA Y TRANSFORMACIÓN CON PANDAS ---
        with open(latest_json, "r") as f:
            data_json = json.load(f)
        
        df = pd.DataFrame(data_json)

        print("⚡ Transformando datos...")
        # Limpieza de tipos de datos
        df['date'] = pd.to_datetime(df['date'])
        df['price'] = pd.to_numeric(df['4. close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['5. volume'], errors='coerce')
        
        # Selección de columnas finales
        df = df[['symbol', 'date', 'price', 'volume']]
        
        # Columna de auditoría
        df['processed_at'] = pd.to_datetime('now')

        print(f"✅ Transformación exitosa: {len(df)} registros procesados.")

        # --- 4. GUARDADO LOCAL Y SUBIDA A AZURE ---
        # Guardar Parquet localmente
        df.to_parquet(temp_parquet, index=False, engine='pyarrow')
        print(f"📦 Parquet generado: {temp_parquet}")

        # Conexión a Azure
        print("☁️ Subiendo a Azure Data Lake (02-silver)...")
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)
        
        blob_name = "02-silver/stock_data.parquet"
        silver_blob = container_client.get_blob_client(blob_name)

        with open(temp_parquet, "rb") as data:
            silver_blob.upload_blob(data, overwrite=True)
        
        print(f"🚀 ¡Éxito! Capa 02-Silver actualizada en la nube.")

    except Exception as e:
        print(f"❌ Error en la transformación: {e}")

if __name__ == "__main__":
    transform_bronze_to_silver()