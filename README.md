# 🚀 ETL Projects

Welcome to my **ETL Projects** repository! I'm Amitesh Awasthi, a data analyst at **Appsquadz Software Pvt. Ltd.**, actively working on cloud-based data pipelines, real-time ingestion, and automation using AWS Glue, Athena, and PySpark. This repository showcases my hands-on work building scalable ETL workflows for analytics, BI, and ML pipelines.

---

## 📌 About Me

I'm a results-driven data professional with a strong background in SQL, Python, and cloud-native tools like AWS Glue, Athena, and S3. I focus on building **automated, cost-effective, and high-performance data solutions**.

This repository is part of my journey toward becoming a **full-stack data engineer**.

---

## 📂 Projects Overview

### 1. **OTT Platform User Analytics**
- **Tech Stack**: AWS Glue, Athena, PySpark, S3, SQL
- **Description**: An ETL pipeline that processes logs and registration data to analyze user behavior across multiple platforms.
- **Output**: Platform-wise download and registration reports delivered via automated email in HTML format.

### 2. **Incremental ETL with Deduplication**
- **Tech Stack**: AWS Glue, Python, SQL
- **Description**: An ETL job that ingests new records into a data lake while avoiding duplicates using logical filtering.

### 3. **BI Dashboard Data Preparation**
- **Tech Stack**: Power BI, Athena, ODBC Connector
- **Description**: Data pipeline that powers a BI dashboard showing real-time user metrics.

---

## ⚙️ Key Features

- ✅ Automated Glue job scheduling and monitoring
- ✅ Secure IAM role-based access via STS
- ✅ SQL templating for Athena queries
- ✅ HTML email reporting via AWS SES
- ✅ Seamless S3 data flow integration

---

## 📈 Skills Demonstrated

- AWS Glue & PySpark scripting
- Athena query optimization
- SQL transformations & aggregations
- S3 integration for raw and processed data
- Data deduplication logic
- Email automation using SMTP & SES
- Git-based project organization

---

## 🔧 How to Run Locally (Dev Only)

> Note: These steps are for testing in local or dev environments.

```bash
# Install required packages
pip install boto3 pandas

# Run a Glue trigger script (example)
python run_glue_job.py

