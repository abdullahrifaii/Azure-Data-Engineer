# Azure Data Engineering Platform with Metadata-Driven Data Quality Framework

![Azure](https://img.shields.io/badge/Azure-0078D4?logo=microsoftazure&logoColor=white)
![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0062AD?logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?logo=mysql&logoColor=white)

## Overview

This project demonstrates an end-to-end Azure Data Engineering solution built using the Medallion Architecture and an integrated Metadata-Driven Data Quality Framework.

The solution ingests Microsoft Dynamics 365 Common Data Model (CDM) datasets, processes them using Azure Data Factory and Azure Databricks, validates data quality through configurable metadata rules, and produces clean, analytics-ready datasets.

The project follows modern data engineering best practices including modular ETL pipelines, incremental loading, reusable validation components, automated monitoring, and scalable cloud architecture.

---

# Solution Architecture

```text
Microsoft Dynamics 365
(Common Data Model)
            │
            ▼
Azure Data Factory
(Orchestration)
            │
            ▼
Azure Data Lake Storage
            │
            ▼
Bronze Layer
(Raw Data)
            │
            ▼
Azure Databricks
(PySpark Transformations)
            │
            ▼
Metadata-Driven
Data Quality Framework
            │
     ┌──────┴─────────┐
     ▼                ▼
Valid Records     Invalid Records
     │                │
     ▼                ▼
Silver Layer     Bad Records
     │
     ▼
Azure SQL Metadata
&
DQ Results
```

---

# Features

## Data Engineering

- End-to-End ETL Pipelines
- Azure Data Factory orchestration
- Azure Databricks transformations
- Medallion Architecture
- Incremental loading
- Watermark processing
- Azure SQL metadata management
- Modular pipeline design

## Data Quality

- Metadata-driven validation framework
- Dynamic rule execution
- Primary Key validation
- Null value validation
- Bad record isolation
- Data Quality reporting
- Centralized metadata management
- Reusable validation engine

---

# Technologies

| Category | Technologies |
|-----------|-------------|
| Cloud | Microsoft Azure |
| Data Integration | Azure Data Factory |
| Data Processing | Azure Databricks |
| Programming | Python, PySpark |
| Database | Azure SQL Database |
| Storage | Azure Data Lake Storage |
| Query Language | SQL |
| Source System | Microsoft Dynamics 365 CDM |
| Automation | Azure Logic Apps |
| DevOps | Azure DevOps |

---

# Medallion Architecture

The project follows a layered data architecture to improve scalability, maintainability, and data quality.

## Bronze Layer

Raw ingestion of source data.

Characteristics:

- Historical preservation
- Minimal transformations
- Incremental ingestion
- Source system replication

---

## Silver Layer

Business-ready standardized datasets.

Characteristics:

- Cleansed data
- Standardized schema
- Data validation
- Incremental updates
- Analytics-ready structure

---

# Metadata-Driven Data Quality Framework

Instead of embedding validation rules inside notebooks or pipelines, the framework stores validation rules in Azure SQL metadata tables.

During execution, the framework dynamically retrieves the configured rules and applies them to the corresponding datasets.

This approach enables new validation rules to be added without modifying pipeline logic.

---

# Data Quality Workflow

```text
Read Metadata Rules
        │
        ▼
Load Dataset
        │
        ▼
Execute Validation Rules
        │
 ┌──────┴──────────┐
 ▼                 ▼
Pass           Fail
 │                 │
 ▼                 ▼
Silver       Bad Records
 │                 │
 └──────────┬──────┘
            ▼
Data Quality Results
```

---

# Current Validation Rules

Implemented rules include:

- Primary Key Check
- Null Value Check

The framework is designed for easy extension with additional validations such as:

- Duplicate Detection
- Range Validation
- Referential Integrity
- Regex Validation
- Custom Business Rules

---

# Data Flow

1. Microsoft Dynamics 365 exports CDM datasets.
2. Azure Data Factory orchestrates ingestion.
3. Data is stored in the Bronze layer.
4. Azure Databricks transforms data using PySpark.
5. Metadata-driven validation rules are retrieved from Azure SQL.
6. Data Quality checks are executed.
7. Invalid records are isolated.
8. Valid data is written to the Silver layer.
9. Data Quality results are stored.
10. Watermarks are updated for incremental processing.

---

# Project Structure

```
AzureDataEngineering/

│

├── Azure Data Factory

│      ├── Pipelines

│      ├── Linked Services

│      ├── Datasets

│

├── Databricks

│      ├── Bronze

│      ├── Silver

│      ├── Data Quality

│

├── SQL

│      ├── Metadata Tables

│      ├── Views

│      ├── Stored Procedures

│

├── Utilities

├── Configuration

└── Documentation
```

---

# Skills Demonstrated

- Azure Data Engineering
- Data Pipeline Development
- ETL / ELT
- Medallion Architecture
- Azure Data Factory
- Azure Databricks
- PySpark
- SQL
- Azure SQL Database
- Incremental Loading
- Watermark Processing
- Metadata-Driven Design
- Data Quality Engineering
- Cloud Architecture
- Azure DevOps Integration
- Azure Logic Apps
- Technical Documentation

---

# Future Improvements

- Gold Layer implementation
- Delta Live Tables
- CI/CD deployment pipeline
- Azure Monitor integration
- Data Quality dashboards
- Additional validation rules
- Performance optimization
- Power BI reporting

---

# Author

**Abdallah El Rifai**

📧 abdullah.rifaii@outlook.com

🔗 LinkedIn: https://linkedin.com/in/abdallah-elrifai

💻 GitHub: https://github.com/abdullahrifaii
