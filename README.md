# Azure Financial Data Pipeline: SCD Type 2 Implementation

## 📌 Descripción del Proyecto
Este proyecto implementa un pipeline de datos de extremo a extremo (End-to-End) diseñado para extraer, transformar y cargar (ETL) datos financieros reales en la nube de **Azure**. El objetivo es demostrar la implementación de una arquitectura **Medallion** y la gestión de historización mediante **Slowly Changing Dimensions (SCD) Tipo 2**.

## 🏗️ Arquitectura Técnica (Cloud Stack)
* **Ingesta:** Python (API Alpha Vantage)
* **Almacenamiento:** Azure Data Lake Storage Gen2 (Capa Bronze, Silver, Gold)
* **Orquestación:** Azure Data Factory
* **Transformación:** Azure Databricks / Spark
* **Data Warehouse:** Azure SQL Database

## 🚀 Fase 1: Extracción y Resiliencia de Datos
En esta primera etapa, se desarrolló un motor de ingesta en Python para obtener datos del mercado bursátil.

### 🛠️ Retos Técnicos y Soluciones Implementadas:
1.  **Seguridad SSL:** Manejo de errores de protocolo SSL en entornos locales mediante gestión de certificados con `urllib3`.
2.  **Gestión de Rate Limiting:** Implementación de **Exponential Backoff** (pausas de 12s) para respetar el límite de 5 peticiones por minuto de la API.
3.  **Estrategia de Volumen:** Pivot de "profundidad histórica" a "amplitud de entidades", logrando un dataset consolidado de **2,500 registros** financieros.
4.  **Seguridad de Credenciales:** Uso de variables de entorno (`.env`) y archivos de plantilla (`.env.example`) para proteger secretos industriales.

## 📂 Estructura del Repositorio
* **`azure/`**: Plantillas e infraestructura como código (ARM/Bicep) para el despliegue de recursos.
* **`data/`**: Almacenamiento local de muestras de datos en formato JSON (Capa Bronze).
* **`notebooks/`**: Procesos de limpieza y transformación avanzada utilizando PySpark.
* **`scripts/`**: Lógica de extracción y scripts auxiliares en Python.
* **`.env.example`**: Plantilla de configuración para replicar el entorno de forma segura.

## 📊 Estructura de Datos (Capa Bronze)
Cada registro extraído contiene:
* `open / high / low / close`: Precios de cotización.
* `volume`: Volumen de transacciones (liquidez).
* `symbol`: Ticker identificador (ej. AAPL, MSFT).
* `date`: Clave temporal para la lógica de historización SCD2.

---
**Autor:** Felipe Morales  
**Rol:** Data Engineer  
**Habilidades:** SQL | PostgreSQL | Data Warehousing | Python | Azure
