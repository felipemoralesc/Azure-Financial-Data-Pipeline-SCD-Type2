import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

# Connection string desde GitHub Secrets
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Nombre del File System (container)
file_system_name = "financial-data"

# Rutas dentro del Data Lake
silver_file_path = "02-silver/stock_cleaned.parquet"
gold_file_path = "03-gold/stock_analytics.parquet"

# Crear cliente del Data Lake
service_client = DataLakeServiceClient.from_connection_string(connection_string)

file_system_client = service_client.get_file_system_client(file_system_name)

print("Conectado a Azure Data Lake")

# Leer archivo Silver
file_client = file_system_client.get_file_client(silver_file_path)

download = file_client.download_file()
data = download.readall()

df = pd.read_parquet(BytesIO(data))

print("Archivo Silver cargado")

# Transformaciones para Gold
gold_df = df.groupby(["Symbol","Date"]).agg(
    avg_price=("Price", "mean"),
    max_price=("Price", "max"),
    min_price=("Price", "min"),
    avg_volume=("Volume", "mean"),
    last_pipeline_run=("Processed_at", "max")
).reset_index()

print("Transformaciones Gold completadas")

# Convertir dataframe a Parquet en memoria
buffer = BytesIO()
gold_df.to_parquet(buffer, index=False)

# Guardar archivo en Gold
gold_file_client = file_system_client.get_file_client(gold_file_path)

gold_file_client.upload_data(buffer.getvalue(), overwrite=True)

print("Archivo Gold generado correctamente en 03-gold")
