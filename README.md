# Azure Financial Data Pipeline: SCD Type 2 Implementation

## 📌 Descripción del Proyecto
Este proyecto consiste en la construcción de un pipeline de datos de extremo a extremo (End-to-End) diseñado para extraer, transformar y cargar (ETL) datos financieros reales en la nube de **Azure**. El objetivo principal es demostrar la implementación de una arquitectura **Medallion** y la gestión de historización mediante **Slowly Changing Dimensions (SCD) Tipo 2**.

## 🏗️ Arquitectura Técnica (En progreso)
* **Ingesta:** Python (API Alpha Vantage)
* **Almacenamiento:** Azure Data Lake Storage Gen2 (Capa Bronze, Silver, Gold)
* **Orquestación:** Azure Data Factory
* **Transformación:** Azure Databricks / Spark
* **Data Warehouse:** Azure SQL Database

## 🚀 Fase 1: Extracción y Resiliencia de Datos
En esta primera etapa, se desarrolló un motor de ingesta en Python para obtener datos del mercado bursátil (S&P 500).

### 🛠️ Retos Técnicos y Soluciones Implementadas:
Para garantizar una extracción robusta, se abordaron los siguientes desafíos:

1.  **Seguridad SSL (Handshake Failure):** Se detectaron errores de protocolo SSL en entornos locales (Python 3.13). Se implementó una solución mediante la gestión de certificados con `urllib3` y `verify=False` para asegurar la conectividad en la fase de desarrollo.
2.  **Gestión de Rate Limiting:** La API gratuita impone un límite de 5 peticiones por minuto. Se integró una lógica de **Exponential Backoff** (pausas programadas de 12 segundos entre peticiones) para evitar bloqueos de IP.
3.  **Estrategia de Volumen (Pivot de Datos):** Debido a las restricciones de cuotas diarias, se optimizó el script para pivotar de un modelo de "profundidad histórica" a uno de "amplitud de entidades", logrando un dataset consolidado de **2,500 registros** financieros (50 empresas x 50-100 días).
4.  **Seguridad de Credenciales:** Implementación de variables de entorno mediante `.env` y protección de secretos con `.gitignore` para evitar la filtración de API Keys.

### 📂 Estructura de la Capa Bronze (Raw JSON)
El dataset extraído presenta la siguiente estructura de metadatos:
* `open/high/low/close`: Precios de la jornada bursátil.
* `volume`: Cantidad de activos transaccionados.
* `symbol`: Ticker identificador de la empresa.
* `date`: Fecha de registro (Clave temporal para SCD2).

## 📂 Estructura del Repositorio
* `scripts/`: Scripts de Python para la extracción inicial.
* `data/`: Muestras de datos en formato JSON (Capa Bronze).
* `notebooks/`: (Próximamente) Transformaciones en Databricks.
* `azure/`: (Próximamente) Plantillas ARM/Bicep para infraestructura.

---
**Autor:** Felipe Morales  
**Rol:** Data Engineer  
**Habilidades:** SQL | Python | Azure | Data Modeling
