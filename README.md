
# FutureCart-CRM-DataMart-Pipeline-AWS

## 📦 Project Overview
This project, developed by Aahash Kamble, is a comprehensive CRM analytics pipeline for FutureCart Inc., a retail and e-commerce company. It leverages AWS services and big data tools to process customer interaction data in both real-time and batch modes, enabling the generation of actionable KPIs.

## 🧭 Architecture Diagram

![Pizza Chain Insights Architecture](images/Pizza%20Chain%20Insights%20Architecture.png)
 
## 🏗️ Architecture
The project follows a Lambda Architecture with the following layers:
- **Extract Layer**: Real-time and batch data ingestion using AWS Kinesis and Python scripts.
- **Landing Layer**: Raw data stored in HDFS via EMR.
- **Transformation Layer**: Data modeled into fact and dimension tables using Hive and Spark.
- **Load Layer**: Final data loaded into Amazon Redshift for analytics and visualization.

## ⚙️ Tech Stack
- AWS EC2, S3, Kinesis, EMR, Redshift
- Hive, HDFS, Spark Structured Streaming
- Python, PySpark
- SQL, JSON

## 🚀 Features
- Real-time and batch data ingestion
- Historical data generation
- KPI computation and reporting
- Hive table creation and transformation
- Redshift integration for analytics

## 📁 Folder Structure
```
Project 1-FutureKart/
├── dimensions/
│   ├── futurecart_calendar_details.txt
│   ├── futurecart_call_center_details.txt
│   ├── futurecart_case_category_details.txt
│   ├── futurecart_case_country_details.txt
│   ├── futurecart_case_priority_details.txt
│   ├── futurecart_employee_details.txt
│   ├── futurecart_product_details.txt
│   ├── futurecart_survey_question_details.txt
│   └── realtimedata/000000_0
├── scripts/
│   ├── generate_historical_data.py
│   ├── kinesis_to_redshift.py
│   └── stream_to_kinesis.py
├── requirements.txt
├── Aahash_Kamble_Project1_Submission.pdf
└── Customer Retention Strategy.pdf
```

## 🛠️ How to Run
1. Launch EC2 and configure MySQL.
2. Upload dimension files to HDFS and create Hive tables.
3. Run `generate_historical_data.py` to simulate batch data.
4. Use `stream_to_kinesis.py` to push real-time events.
5. Run `kinesis_to_redshift.py` using Spark on EMR.
6. Query Redshift to generate KPIs.

## 👤 Author
**Aahash Kamble**  
Project 1 Submission for CRM Analytics Pipeline

