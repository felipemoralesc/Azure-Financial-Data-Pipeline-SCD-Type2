# Azure Financial Data Pipeline: SCD Type 2 Implementation

## 📌 Descripción del Proyecto
Este proyecto implementa un pipeline de datos de extremo a extremo diseñado para extraer, transformar y cargar (ETL) datos financieros reales en la nube de **Azure**. El objetivo es demostrar la implementación de una arquitectura **Medallion** y la gestión de historización mediante **Slowly Changing Dimensions (SCD) Tipo 2**.

## 🏗️ Arquitectura Técnica (Cloud Stack)
* **Ingesta:** Python (API Alpha Vantage)
* **Almacenamiento:** Azure Data Lake Storage Gen2 (Capa Bronze, Silver, Gold)
* **Orquestación:** Azure Data Factory (Pendiente)
* **Transformación:** Pandas / PySpark
* **Data Warehouse:** Azure SQL Database (Pendiente)

## 🚀 Fase 1: Ingesta Masiva y Resiliencia de Datos
En esta etapa, se desarrolló un motor de ingesta robusto para consolidar datos de las 50 empresas más importantes del S&P 500.

### 🛠️ Retos Técnicos y Soluciones Implementadas:
1. **Resiliencia en Nube:** Implementación de `connection_timeout=600` en la librería `azure-storage-blob` para asegurar la integridad de la carga de archivos pesados ante inestabilidades de red.
2. **Gestión de Rate Limiting:** Implementación de pausas activas (12s) para cumplir con el límite de la API gratuita, logrando una extracción masiva de **5,000 registros** sin bloqueos.
3. **Seguridad SSL:** Manejo de excepciones de protocolo SSL mediante `urllib3` para garantizar la conectividad en diversos entornos de red.
4. **Seguridad de Credenciales:** Uso de variables de entorno (`.env`) y plantillas (`.env.example`) para proteger secretos de infraestructura.

## 📂 Estructura del Repositorio
* **`azure/`**: Configuraciones y despliegues de recursos cloud.
* **`data/`**: Directorio para backups locales (incluye `.gitkeep` para mantener estructura sin subir datos pesados).
* **`scripts/`**: 
    * `extract_massive_data.py`: Motor de ingesta masiva (API -> Bronze).
    * `bronze_to_silver.py`: Lógica de transformación a Parquet (Bronze -> Silver).
* **`.env.example`**: Plantilla para replicar el entorno de forma segura.

## 📊 Estado de las Capas (Data Lake)
* **Capa 01-Bronze:** Almacenamiento de archivos JSON crudos con la respuesta íntegra de la API.
* **Capa 02-Silver (En proceso):** Transformación de JSON a **Parquet** utilizando Pandas, optimizando tipos de datos y preparando la estructura para el modelo dimensional.

---
**Autor:** Felipe Morales  
**Rol:** Data Engineer  
**Habilidades:** SQL | Data Warehousing | Python | Azure Cloud
