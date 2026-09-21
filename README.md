# Enterprise Spark Data Quality & Reconciliation Framework

## Overview
This repository showcases an enterprise-grade data pipeline architecture designed to handle large-scale data ingestion, high-cardinality data skew optimization, and automated source-to-target reconciliation between operational databases (such as Oracle/SQL Server) and cloud data lakes (Databricks / Delta Lake).

## Key Architectural Features

### 1. Salting for High-Cardinality Data Skew
* **Problem:** Uneven distribution of primary keys (`merchant_id` or `customer_id`) causes severe data skew, bottlenecking executor nodes and causing Out-Of-Memory (OOM) failures in Spark.
* **Solution:** Implemented custom dynamic salting mechanisms to distribute skewed partitions evenly across worker nodes before executing joins, ensuring optimal cluster resource utilization.

### 2. Robust Source-to-Target Reconciliation
* Avoids dangerous default inner joins (which silently drop missing records).
* Utilizes **Full Outer Joins** and **Left Anti-Joins** to audit and detect:
  * **Data Loss:** Records existing in the source but missing in the target.
  * **Orphan Records:** Unmatched target entries.
  * **Value Mismatches:** Column-by-column drift detection across datasets.

### 3. Handling Deletions & Modifications
* Tracks old and modified records using `updated_at` incremental watermarking.
* Employs primary key anti-sets to catch upstream deletions that traditional date-filtered row counts miss.

## Tech Stack
* **Languages:** Scala, Python (PySpark)
* **Frameworks:** Apache Spark, Databricks, Delta Lake
* **Paradigms:** Distributed Computing, ETL Pipeline Design, Data Observability & Quality Assertions
