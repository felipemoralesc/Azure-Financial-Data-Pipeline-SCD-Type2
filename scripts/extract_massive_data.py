import os
import requests
import json
import time
import urllib3
from datetime import datetime
from pathlib import Path  # <--- Agregado
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS (Blindaje para Azure/GitHub) ---
# Detecta la carpeta del script (scripts/) y sube un nivel a la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Carga el .env usando la ruta absoluta calculada en tiempo real
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Deshabilitar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Variables de entorno (Ahora seguras porque load_dotenv ya tiene la ruta clara)
API_KEY = os.getenv("STOCK_API_KEY")
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

# Lista de símbolos (se mantiene igual)
symbols = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK.B", "V", "JNJ",
    "WMT", "JPM", "MA", "PG", "UNH", "HD", "DIS", "PYPL", "BAC", "VZ",
    "ADBE", "CMCSA", "NFLX", "KO", "PFE", "INTC", "T", "PEP", "ABT", "CSCO",
    "XOM", "CVX", "NKE", "MRK", "CRM", "AVGO", "WFC", "ACN", "COST", "MCD",
    "TMO", "MDT", "LLY", "DHR", "NEE", "TXN", "HON", "UPS", "QCOM", "LIN"
]

def extract_and_load_massive():
    all_data = []
    print(f"🚀 Iniciando extracción masiva desde: {BASE_DIR}")

    # --- PARTE 1: EXTRACCIÓN ---
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
            
            time.sleep(12) # Respeto a la API (Free Tier)
        except Exception as e:
            print(f"❗ Error en {sym}: {e}")

    # --- PARTE 2: GUARDADO LOCAL (USANDO RUTA DINÁMICA) ---
    filename = f"massive_stock_data_{datetime.now().strftime('%Y%m%d')}.json"
    
    # Aquí usamos BASE_DIR para que la carpeta 'data' siempre se encuentre
    local_data_dir = os.path.join(BASE_DIR, "data")
    os.makedirs(local_data_dir, exist_ok=True)
    local_path = os.path.join(local_data_dir, filename)

    with open(local_path, "w") as f:
        json.dump(all_data, f, indent=4)
    
    print(f"\n✨ Archivo local generado en: {local_path}")

    # --- PARTE 3: CARGA A AZURE ---
    try:
        print("☁️ Conectando con Azure Data Lake...")
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_name = f"01-bronze/{filename}"
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        print(f"🚀 Subiendo a la capa Bronze...")
        with open(local_path, "rb") as data_file:
            blob_client.upload_blob(data_file, overwrite=True, connection_timeout=600)
        
        print(f"✅ ¡Misión cumplida! Datos en Azure: {blob_name}")
    except Exception as e:
        print(f"❌ Error al subir a Azure: {e}")

if __name__ == "__main__":
    extract_and_load_massive()