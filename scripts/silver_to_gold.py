import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

# Variables de entorno
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

file_system = "financial-data"

silver_file_path = "02-silver/stock_data.parque"
gold_file_path = "03-gold/stock_analytics.parquet"

# Conexión al Data Lake
service_client = DataLakeServiceClient.from_connection_string(connection_string)
file_system_client = service_client.get_file_system_client(file_system)

# Leer archivo Silver
file_client = file_system_client.get_file_client(silver_file_path)

download = file_client.download_file()
data = download.readall()

df = pd.read_parquet(BytesIO(data))

print("Datos cargados desde Silver")

# Transformaciones para Gold
gold_df = df.groupby("symbol").agg(
    avg_price=("price", "mean"),
    max_price=("price", "max"),
    min_price=("price", "min"),
    avg_volume=("volume", "mean"),
    last_update=("timestamp", "max")
).reset_index()

print("Agregaciones calculadas")

# Guardar resultado como Parquet
buffer = BytesIO()
gold_df.to_parquet(buffer, index=False)

# Subir archivo a Gold
gold_file_client = file_system_client.get_file_client(gold_file_path)

gold_file_client.upload_data(buffer.getvalue(), overwrite=True)

print("Archivo Gold generado correctamente")
