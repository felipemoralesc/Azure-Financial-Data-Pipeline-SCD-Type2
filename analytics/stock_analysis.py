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
    # 1. LISTAR ARCHIVOS GOLD
    # ==========================================

    blobs = list(container_client.list_blobs(name_starts_with=GOLD_PATH))

    if not blobs:
        raise Exception("No hay datos en Gold para analizar")

    print(f"📂 Archivos encontrados en Gold: {len(blobs)}")

    # ==========================================
    # 2. LEER TODOS LOS PARQUET
    # ==========================================

    dfs = []

    for blob in blobs:

        if blob.name.endswith(".parquet"):

            print(f"📥 Leyendo: {blob.name}")

            blob_client = container_client.get_blob_client(blob.name)

            data = blob_client.download_blob().readall()

            df = pd.read_parquet(BytesIO(data))

            dfs.append(df)

    df_all = pd.concat(dfs, ignore_index=True)

    print(f"📊 Total registros cargados: {len(df_all)}")

    # ==========================================
    # 3. MÉTRICAS DE NEGOCIO
    # ==========================================

    print("\n📈 MÉTRICAS GENERADAS:")

    # Precio promedio por símbolo
    avg_price = df_all.groupby("symbol")["price"].mean().sort_values(ascending=False)
    print("\n💰 Precio promedio por símbolo:")
    print(avg_price)

    # Volumen total por símbolo
    total_volume = df_all.groupby("symbol")["volume"].sum().sort_values(ascending=False)
    print("\n📦 Volumen total por símbolo:")
    print(total_volume)

    # Último precio por símbolo
    latest_price = (
        df_all.sort_values("date")
        .groupby("symbol")
        .tail(1)
        .set_index("symbol")["price"]
    )

    print("\n🕒 Último precio registrado:")
    print(latest_price)

    # ==========================================
    # 4. TOP MOVERS (simulación analítica)
    # ==========================================

    price_variation = (
        df_all.sort_values("date")
        .groupby("symbol")
        .agg(first_price=("price", "first"), last_price=("price", "last"))
    )

    price_variation["change_%"] = (
        (price_variation["last_price"] - price_variation["first_price"])
        / price_variation["first_price"]
    ) * 100

    print("\n🚀 Top ganadores (% cambio):")
    print(price_variation.sort_values("change_%", ascending=False).head())

    print("\n📉 Top perdedores (% cambio):")
    print(price_variation.sort_values("change_%", ascending=True).head())

    print("\n✅ Análisis completado correctamente")


# ==========================================
# EJECUCIÓN
# ==========================================

if __name__ == "__main__":
    analyze_gold_data()
