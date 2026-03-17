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
gold_base_path = "03-gold"

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
# 3. Normalizar columnas
# ==========================================

df.columns = df.columns.str.lower().str.strip()

print("Columnas:", df.columns)


# ==========================================
# 4. Validar columnas
# ==========================================

required_columns = ["symbol", "date", "price", "volume", "processed_at"]

missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    raise Exception(f"Faltan columnas: {missing_columns}")


# ==========================================
# 5. Asegurar tipo fecha
# ==========================================

df["date"] = pd.to_datetime(df["date"])


# ==========================================
# 6. Transformaciones GOLD
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
# 7. Crear columnas de partición
# ==========================================

gold_df["year"] = gold_df["date"].dt.year
gold_df["month"] = gold_df["date"].dt.month
gold_df["day"] = gold_df["date"].dt.day


# ==========================================
# 8. Guardar por particiones
# ==========================================

for (year, month, day), partition_df in gold_df.groupby(["year", "month", "day"]):

    # Crear ruta dinámica
    partition_path = f"{gold_base_path}/year={year}/month={month:02d}/day={day:02d}/stock_analytics.parquet"

    print(f"Guardando partición: {partition_path}")

    buffer = BytesIO()
    partition_df.to_parquet(buffer, index=False)

    file_client = file_system_client.get_file_client(partition_path)

    file_client.upload_data(buffer.getvalue(), overwrite=True)

print("Proceso Gold con particionamiento completado")
