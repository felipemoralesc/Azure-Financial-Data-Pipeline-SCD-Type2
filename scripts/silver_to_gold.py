import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

# ==========================================
# 1. Conexión a Azure Data Lake
# ==========================================

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

file_system_name = "datalake"

silver_file_path = "02-silver/stock_data.parquet"
gold_file_path = "03-gold/stock_analytics.parquet"

service_client = DataLakeServiceClient.from_connection_string(connection_string)

file_system_client = service_client.get_file_system_client(file_system_name)

print("Conectado a Azure Data Lake")


# ==========================================
# 2. Leer archivo desde SILVER
# ==========================================

file_client = file_system_client.get_file_client(silver_file_path)

download = file_client.download_file()
data = download.readall()

df = pd.read_parquet(BytesIO(data))

print("Archivo Silver cargado")


# ==========================================
# 3. Normalizar nombres de columnas
# ==========================================

df.columns = df.columns.str.lower().str.strip()

print("Columnas detectadas:", df.columns)


# ==========================================
# 4. Validar columnas necesarias
# ==========================================

required_columns = ["symbol", "date", "price", "volume", "processed_at"]

missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    raise Exception(f"Faltan columnas en el dataset: {missing_columns}")


# ==========================================
# 5. Convertir timestamp a fecha real
# ==========================================

df["date"] = pd.to_datetime(df["date"], unit="ms")


# ==========================================
# 6. Transformaciones para GOLD
# ==========================================

gold_df = df.groupby(["symbol", "date"]).agg(
    avg_price=("price", "mean"),
    max_price=("price", "max"),
    min_price=("price", "min"),
    avg_volume=("volume", "mean"),
    last_pipeline_run=("processed_at", "max")
).reset_index()

print("Transformaciones Gold completadas")


# ==========================================
# 7. Convertir dataframe a parquet
# ==========================================

buffer = BytesIO()
gold_df.to_parquet(buffer, index=False)


# ==========================================
# 8. Guardar en carpeta GOLD
# ==========================================

gold_file_client = file_system_client.get_file_client(gold_file_path)

gold_file_client.upload_data(buffer.getvalue(), overwrite=True)

print("Archivo Gold generado correctamente en 03-gold")
