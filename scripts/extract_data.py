import os
import requests
import json
import time
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
API_KEY = os.getenv("STOCK_API_KEY")

# Lista de las 50 empresas más importantes (S&P 500)
symbols = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK.B", "V", "JNJ",
    "WMT", "JPM", "MA", "PG", "UNH", "HD", "DIS", "PYPL", "BAC", "VZ",
    "ADBE", "CMCSA", "NFLX", "KO", "PFE", "INTC", "T", "PEP", "ABT", "CSCO",
    "XOM", "CVX", "NKE", "MRK", "CRM", "AVGO", "WFC", "ACN", "COST", "MCD",
    "TMO", "MDT", "LLY", "DHR", "NEE", "TXN", "HON", "UPS", "QCOM", "LIN"
]

def extract_massive_stocks():
    all_data = []
    print(f"🚀 Iniciando extracción masiva de {len(symbols)} empresas...")

    for i, sym in enumerate(symbols):
        # Usamos 'compact' que es la versión GRATIS
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={sym}&outputsize=compact&apikey={API_KEY}'
        
        try:
            response = requests.get(url, verify=False, timeout=10)
            data = response.json()
            
            if "Time Series (Daily)" in data:
                # Añadimos el símbolo a cada registro para saber de qué empresa es
                for date, values in data["Time Series (Daily)"].items():
                    values["symbol"] = sym
                    values["date"] = date
                    all_data.append(values)
                
                print(f"✅ {i+1}/{len(symbols)}: {sym} cargado.")
            else:
                print(f"⚠️ No se pudo cargar {sym}. Posible límite de API.")
            
            # Alpha Vantage Free permite 5 peticiones por minuto. 
            # Para no fallar, esperamos 12 segundos entre cada una.
            time.sleep(12)

        except Exception as e:
            print(f"❗ Error en {sym}: {e}")

    # Guardamos el "Ladrillo" de 5,000 filas
    with open("stock_massive_data.json", "w") as f:
        json.dump(all_data, f, indent=4)
    
    print(f"\n✨ ¡Misión cumplida! Archivo generado con {len(all_data)} registros.")

if __name__ == "__main__":
    extract_massive_stocks()