# Azure Medallion Data Engineering Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?logo=microsoftazure)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange)
![ADF](https://img.shields.io/badge/Azure-Data%20Factory-blue)
![Databricks](https://img.shields.io/badge/Azure-Databricks-red)

## Overview

This project demonstrates the implementation of an end-to-end Azure Data Engineering solution using the Medallion Architecture to ingest, transform, and prepare Microsoft Dynamics 365 Common Data Model (CDM) datasets for analytics.

The solution follows modern data engineering best practices by separating data into Bronze and Silver layers while supporting incremental data loading, scalable transformations, and centralized orchestration.

---

## Architecture

```text
Dynamics 365 CDM Files
          │
          ▼
Azure Data Factory
          │
          ▼
Azure Data Lake Storage
          │
          ▼
Bronze Layer
(Raw Ingestion)
          │
          ▼
Azure Databricks
(PySpark Transformations)
          │
          ▼
Silver Layer
(Cleaned & Standardized Data)
          │
          ▼
Azure SQL Metadata
```

---

## Features

- End-to-end ETL pipeline
- Medallion Architecture
- Azure Data Factory orchestration
- Azure Databricks transformations
- PySpark processing
- Incremental data loading
- Watermark-based ingestion
- Azure SQL metadata management
- Modular pipeline design

---

## Technologies

- Azure Data Factory
- Azure Databricks
- PySpark
- Azure SQL Database
- Azure Data Lake Storage
- Microsoft Dynamics 365 CDM
- SQL

---

## Project Structure

```
Datasets/
    Hr/
    Purchase/
    Sales/
    Others/

Pipelines/
    Data ingestion
    Incremental processing

Databricks/
    Bronze transformations
    Silver transformations

Metadata/
    Watermark configuration
```

---

## Data Flow

1. Source data is extracted from Microsoft Dynamics 365 CDM datasets.
2. Azure Data Factory orchestrates ingestion.
3. Data is stored in the Bronze layer.
4. Azure Databricks transforms and cleans the data.
5. Standardized datasets are written to the Silver layer.
6. Metadata and watermarks are updated in Azure SQL.

---

## Key Concepts

### Bronze Layer

- Raw data ingestion
- Historical preservation
- Minimal transformation

### Silver Layer

- Data cleansing
- Standardization
- Incremental processing
- Business-ready datasets

---

## Skills Demonstrated

- Azure Data Engineering
- ETL Development
- Data Pipeline Design
- PySpark Development
- Incremental Loading
- Medallion Architecture
- Azure Cloud Services
- SQL Development
- Metadata Management

---

## Future Improvements

- Gold Layer implementation
- Delta Live Tables
- CI/CD deployment
- Automated monitoring
- Data quality integration
- Power BI reporting

---

## Author

**Abdallah El Rifai**

LinkedIn: https://linkedin.com/in/abdallah-elrifai

GitHub: https://github.com/abdullahrifaii
