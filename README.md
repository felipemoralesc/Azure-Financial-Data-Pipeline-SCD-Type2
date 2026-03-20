# 🚀 Azure Financial Data Pipeline — End-to-End (Medallion Architecture)

Pipeline de datos end-to-end construido en Azure siguiendo arquitectura Medallion (`Bronze` → `Silver` → `Gold`), con procesamiento incremental, control mediante metadata y consumo analítico en Power BI.

## 🎯 Objetivo del proyecto

Diseñar e implementar un **data pipeline cloud-native** que simule un escenario real de negocio:

* Ingesta de datos desde API (Alpha Vantage)
* Procesamiento incremental (metadata-driven)
* Validaciones de calidad de datos
* Almacenamiento optimizado en Data Lake
* Consumo analítico en Power BI

## 🧩 Arquitectura del pipeline
```text
API (Stock Data)
   ↓
Azure Data Lake Storage Gen2
   ↓
Bronze Layer (Raw JSON)
   ↓
Silver Layer (Clean + Incremental Parquet)
   ↓
Gold Layer (Aggregated + Partitioned)
   ↓
Power BI Dashboard
```

### 🏗️ Medallion Architecture

## 🥉 Bronze Layer — Raw Data

* Datos crudos en formato JSON
* Sin transformaciones
* Histórico completo (Single Source of Truth)

## 🥈 Silver Layer — Clean & Incremental

* Datos limpios y estandarizados
* Eliminación de duplicados
* Conversión de tipos (string → numeric)
* Procesamiento incremental basado en metadata

## 🥇 Gold Layer — Business Ready

* Datos agregados para analítica
* KPIs calculados (ej: change_pct)
* Particionamiento por fecha:
`03-gold/stocks/year=YYYY/month=MM/day=DD/`
* Optimización para consultas (partition pruning)

## 📊 Consumption Layer (Power BI)

* Generación de datasets optimizados
* Consumo desacoplado del pipeline
* Dashboard enfocado en análisis de mercado

## ⚙️ Features principales
**🔥 Incremental Ingestion (Metadata-driven)**

* Procesa solo archivos nuevos
* Evita reprocesamiento
* Pipeline idempotente

## ✅ Data Quality Checks

**Validaciones implementadas:**

* `symbol` no nulo
* `date` válido
* `price > 0`
* `volume >= 0`
* Eliminación de duplicados

## 🧱 Data Lake Optimization

* Uso de Parquet en Silver
* Particionamiento en Gold
* Optimizado para analítica

## 🔄 Orquestación automática

* Pipeline ejecutado con GitHub Actions
* Flujo completo:

```text Extract → Bronze → Silver → Gold```

## 📝 Logging & Monitoring

* Logs estructurados por ejecución:
* registros procesados
* válidos / inválidos
* tiempo de ejecución
* Persistencia en Data Lake:
```texto logs/bronze_to_silver_YYYYMMDD.log```

## 🗂️ Metadata Management

Control de archivos procesados

Evita reprocesamiento

Soporte para incremental ingestion

metadata/processed_files.json
📂 Estructura del Data Lake
01-bronze/    → Datos crudos
02-silver/    → Datos limpios (Parquet)
03-gold/      → Datos analíticos particionados
04-analytics/ → Consumo / análisis específicos
logs/         → Logs del pipeline
metadata/     → Control de procesamiento
📁 Estructura del repositorio
.
├── .github/workflows/pipeline.yml
├── scripts/
│   ├── extract_massive_data.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── powerbi/
│   └── stock_analysis.py
├── data/
│   └── sample/
│       ├── bronze_sample.json
│       └── silver_sample.json
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
📊 Sample Data (Bronze vs Silver)

Ejemplo de transformación:

Bronze (raw API)
{
  "1. open": "249.4000",
  "2. high": "251.8300"
}
Silver (clean)
{
  "open": 249.40,
  "high": 251.83
}
📈 Data Consumption (Power BI)

Dashboard enfocado en análisis financiero:

Variación porcentual de acciones

Top & Bottom performers

Promedio del mercado

Visualización ejecutiva (storytelling)

📸 (Agregar screenshot del dashboard aquí)

🛠️ Tecnologías utilizadas

Azure Data Lake Storage Gen2

Python

Pandas

GitHub Actions

Power BI

💡 Decisiones técnicas clave

Procesamiento incremental en Silver (no en extract)

Uso de metadata para control de estado

Separación clara por capas (Medallion)

Logging persistente para observabilidad

Consumo desacoplado del pipeline

🚀 Ejecución del pipeline

El pipeline se ejecuta automáticamente mediante GitHub Actions:

Trigger manual o programado

Uso de secrets para credenciales

Integración directa con Data Lake

💬 Sobre este proyecto

Este proyecto demuestra habilidades en:

Data Engineering

Diseño de pipelines

Procesamiento incremental

Modelado de datos para analítica

Integración con herramientas de BI

👤 Autor

Felipe Morales
Data Engineer
