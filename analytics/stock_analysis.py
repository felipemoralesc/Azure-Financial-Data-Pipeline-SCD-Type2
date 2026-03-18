import pandas as pd
import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

# ==========================================
# CONFIGURACIÓN
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(os.path.join(BASE_DIR, ".env"))

AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

GOLD_PATH = "03-gold/"
ANALYTICS_PATH = "04-analytics/"

# ==========================================
# FUNCIÓN PARA GUARDAR PARQUET EN AZURE
# ==========================================

def upload_parquet(df, path, container_client):

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob_client = container_client.get_blob_client(path)
    blob_client.upload_blob(buffer, overwrite=True)

    print(f"📤 Guardado: {path}")

# ==========================================
# FUNCIÓN PRINCIPAL
# ==========================================

def analyze_gold_data():

    print("📊 Iniciando análisis de datos (Gold Layer)...")

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_CONNECTION_STRING
    )

    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # ==========================================
    # 1. LEER GOLD
    # ==========================================

    blobs = list(container_client.list_blobs(name_starts_with=GOLD_PATH))

    dfs = []

    for blob in blobs:
        if blob.name.endswith(".parquet"):

            print(f"📥 Leyendo: {blob.name}")

            data = container_client.get_blob_client(blob.name).download_blob().readall()
            df = pd.read_parquet(BytesIO(data))

            dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)

    print(f"📊 Total registros: {len(df_all)}")

    # ==========================================
    # 2. MÉTRICAS
    # ==========================================

    # 💰 Precio promedio (promedio del Avg_price)
    avg_price = (
    df_all.groupby("Symbol")["Avg_price"]
    .mean()
    .reset_index()
    .sort_values(by="Avg_price", ascending=False)
    )

    # 📦 Volumen total (suma del volumen promedio diario)
    total_volume = (
    df_all.groupby("Symbol")["Avg_volume"]
    .sum()
    .reset_index()
    .sort_values(by="Avg_volume", ascending=False)
    )

    # 🚀 Variación de precio
    price_variation = (
    df_all.sort_values("Date")
    .groupby("Symbol")
    .agg(
        first_price=("Avg_price", "first"),
        last_price=("Avg_price", "last")
        )
    .reset_index()
    )

    price_variation["change_pct"] = (
    (price_variation["last_price"] - price_variation["first_price"])
    / price_variation["first_price"]
    ) * 100

    # ==========================================
    # 3. GUARDAR RESULTADOS (DATA MART)
    # ==========================================

    upload_parquet(avg_price, ANALYTICS_PATH + "avg_price.parquet", container_client)
    upload_parquet(total_volume, ANALYTICS_PATH + "total_volume.parquet", container_client)
    upload_parquet(price_variation, ANALYTICS_PATH + "price_variation.parquet", container_client)

    print("✅ Data mart generado correctamente")

# ==========================================
# EJECUCIÓN
# ==========================================

if __name__ == "__main__":
    analyze_gold_data()
