
# 🛠️ ETL Pipeline with AWS Glue, Aurora MySQL & Athena

This project demonstrates a real-world, end-to-end **ETL pipeline** built using **AWS Glue**, **Aurora MySQL**, **Athena**, and **Python automation (boto3)**. It consists of two Glue jobs executed sequentially via a Python script to manage orchestration and credential security using STS.

---

## 🔄 Project Workflow Overview

### 📍 Step-by-step Breakdown

#### ✅ Step 1: **Extract Data from Aurora MySQL**
- A Glue job reads the `meta_information` table from Aurora MySQL using a pre-configured Glue connection (`cms sandbox connection`).
- Converts it to a Spark DataFrame.
- Appends a new column `modified_at` using `current_timestamp()` for traceability.

#### ✅ Step 2: **Transform & Load to S3 (Data Lake)**
- Transformed data is written to an S3 path:
  ```
  s3://pb-etl-athena/analytics_etl/Pb_meta_information/
  ```
- Format: **Parquet** (compressed with Snappy)
- Glue Data Catalog is updated with table name: `Pb_meta_information` under database `analyticsdatabase`

#### ✅ Step 3: **Secure Execution using STS (Temporary Credentials)**
- A Python script assumes an IAM role (`Role_STS`) via AWS STS.
- Temporary credentials are used to securely initiate Glue jobs.

#### ✅ Step 4: **Run Two Jobs Sequentially**
1. **`pb_meta_info`** → Handles extraction & transformation from Aurora to S3.
2. **`PB_REGISTERED_USERS_DOWNLOADS_MAILER`** → Likely formats and sends out HTML-based report via SES (email).

The script polls each job for completion status before proceeding, ensuring the downstream job doesn’t run unless the upstream has succeeded.

---

## 📁 File Structure
```
├── glue_jobs/
│   ├── pb_meta_info_script.py
│   └── PB_REGISTERED_USERS_DOWNLOADS_MAILER.py (optional)
├── trigger_jobs_sequentially.py
└── README.md
```

---

## 🔐 IAM & Role Assumption
- Credentials are fetched **safely** using `boto3` and `assume_role()`
- This ensures cross-account role access without exposing permanent credentials

---

## ⚙️ Technologies Used
| Tool        | Purpose                              |
|-------------|---------------------------------------|
| AWS Glue    | ETL orchestration                     |
| Athena      | Querying transformed data             |
| S3          | Data Lake (Parquet storage)           |
| Aurora MySQL| Source database                       |
| PySpark     | DataFrame transformation              |
| Python + boto3 | Job execution + IAM role handling  |

---

## 📬 Automation Outcome
- A complete data pipeline that:
  - Extracts and transforms data
  - Loads to a partitioned, queryable S3 path
  - Sends formatted reports via HTML email (second job)

---

## 👨‍💻 Author
**Amitesh Awasthi**  
Data Analyst @ Appsquadz Software Pvt. Ltd.  
Working on BI Automation, AWS Glue ETL, and SQL analytics.

- [LinkedIn](https://www.linkedin.com/in/amitesh-awasthi)
- [GitHub](https://github.com/amiteshawasthi)

---

## ⭐️ How to Contribute / Use
- Clone the repo and modify connection names
- Set IAM roles, update job names, and S3 paths accordingly
- Run `trigger_jobs_sequentially.py` after uploading both Glue jobs

---

Feel free to fork this repo and adapt it to your own use cases. Don’t forget to ⭐ it if you find it helpful!
