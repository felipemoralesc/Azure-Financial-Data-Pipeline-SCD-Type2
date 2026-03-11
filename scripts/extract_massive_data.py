import os
import requests
import json
import time
import urllib3
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Configuración inicial
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# Variables de entorno
API_KEY = os.getenv("STOCK_API_KEY")
CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "datalake"

# Lista de las 50 empresas
symbols = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK.B", "V", "JNJ",
    "WMT", "JPM", "MA", "PG", "UNH", "HD", "DIS", "PYPL", "BAC", "VZ",
    "ADBE", "CMCSA", "NFLX", "KO", "PFE", "INTC", "T", "PEP", "ABT", "CSCO",
    "XOM", "CVX", "NKE", "MRK", "CRM", "AVGO", "WFC", "ACN", "COST", "MCD",
    "TMO", "MDT", "LLY", "DHR", "NEE", "TXN", "HON", "UPS", "QCOM", "LIN"
]

def extract_and_load_massive():
    all_data = []
    print(f"🚀 Iniciando extracción masiva de {len(symbols)} empresas...")

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
                print(f"⚠️ No se pudo cargar {sym}. Posible límite o error de API.")
            
            # Respetamos el límite de 5 peticiones por minuto (12 seg de espera)
            time.sleep(12)

        except Exception as e:
            print(f"❗ Error en {sym}: {e}")

    # --- PARTE 2: GUARDADO LOCAL ---
    filename = f"massive_stock_data_{datetime.now().strftime('%Y%m%d')}.json"
    local_path = os.path.join("data", filename)
    os.makedirs("data", exist_ok=True)

    with open(local_path, "w") as f:
        json.dump(all_data, f, indent=4)
    
    print(f"\n✨ Archivo local generado con {len(all_data)} registros.")

    # --- PARTE 3: CARGA A AZURE ---
    try:
        print("☁️ Conectando con Azure Data Lake...")
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        
        # Ruta en el Data Lake
        blob_name = f"01-bronze/{filename}"
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        print(f"🚀 Subiendo '{filename}' a la capa Bronze...")
        with open(local_path, "rb") as data_file:
            blob_client.upload_blob(data_file, overwrite=True, connection_timeout=600)
        
        print(f"✅ ¡Misión cumplida! Datos disponibles en Azure: {blob_name}")

    except Exception as e:
        print(f"❌ Error al subir a Azure: {e}")

if __name__ == "__main__":
    extract_and_load_massive()