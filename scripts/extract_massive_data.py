import os
import requests
import json
import time
import urllib3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS (compatible local / cloud) ---
BASE_DIR = Path(__file__).resolve().parent.parent

# Cargar variables de entorno
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Deshabilitar advertencias SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Variables de entorno
API_KEY = os.getenv("STOCK_API_KEY")
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

# Lista de símbolos
symbols = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK.B", "V", "JNJ",
    "WMT", "JPM", "MA", "PG", "UNH", "HD", "DIS", "PYPL", "BAC", "VZ",
    "ADBE", "CMCSA", "NFLX", "KO", "PFE"
]

def extract_and_load_massive():
    all_data = []
    print("🚀 Iniciando extracción masiva desde Alpha Vantage")

    # --- PARTE 1: EXTRACCIÓN DESDE API ---
    for i, sym in enumerate(symbols):
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={sym}&outputsize=compact&apikey={API_KEY}'
        try:
            response = requests.get(url, verify=False, timeout=10)
            data = response.json()

            if "Time Series (Daily)" in data:
                for date, values in data["Time Series (Daily)"].items():
                    values["symbol"] = sym
                    values["date"] = date
                    all_data.append(values)

                print(f"✅ {i+1}/{len(symbols)}: {sym} cargado.")

            else:
                print(f"⚠️ No se pudo cargar {sym}. Límite de API alcanzado.")

            # Respeto al rate limit del Free Tier
            time.sleep(12)

        except Exception as e:
            print(f"❗ Error en {sym}: {e}")

    print(f"\n📊 Total de registros recolectados: {len(all_data)}")

    # --- PARTE 2: CARGA DIRECTA A AZURE DATA LAKE ---
    try:
        print("☁️ Conectando con Azure Data Lake...")

        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

        filename = f"massive_stock_data_{datetime.now().strftime('%Y%m%d')}.json"
        blob_name = f"01-bronze/{filename}"

        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob=blob_name
        )

        print("🚀 Subiendo datos a la capa Bronze...")

        json_data = json.dumps(all_data)

        blob_client.upload_blob(
            json_data,
            overwrite=True,
            connection_timeout=600
        )

        print(f"✅ ¡Misión cumplida! Datos almacenados en Azure: {blob_name}")

    except Exception as e:
        print(f"❌ Error al subir a Azure: {e}")

if __name__ == "__main__":
    extract_and_load_massive()
