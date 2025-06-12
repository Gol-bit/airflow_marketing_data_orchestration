# airflow_marketing_data_orchestration

# Marketing Data Pipeline Project (Airflow, Python, PostgreSQL, Google BQ)

This repository contains a collection of Python scripts and Airflow DAGs that automate data ingestion, processing, and aggregation for gaming app analytics. All pipelines run on Airflow, secrets are securely stored in Airflow Variables, raw data lands in PostgreSQL, and aggregated metrics are pushed to Google BigQuery for reporting and visualization in Looker.

## 📂 Repository Structure

```
your-project/
├── .gitignore           # files to ignore in git
├── README.md            # this documentation
├── requirements.txt     # Python dependencies
├── dags/                # Airflow DAG definitions
│   ├── raw_mintegral_dag.py
│   ├── raw_af_dag.py
│   ├── raw_unity_dag.py
│   ├── agg_google_dag.py
│   └── backfill_raw_af_dag.py
├── scripts/             # ETL script implementations
│   ├── unity_raw.py
│   ├── mintegral_raw.py
│   ├── af_raw.py
│   └── agg_google.py

```

## 🚀 How It Works

1. **Airflow as Orchestrator**

   * All DAGs live in the `dags/` folder and are scheduled or triggered manually.
   * Secrets (API keys, DB URLs) are managed via Airflow Variables, never hard‑coded.

2. **Raw Data Ingestion**

   * Scripts under `scripts/` connect to various ad platforms: Unity, Mintegral, AppsFlyer, Google Campaign Manager (via BigQuery)
   * Data is fetched daily (or in backfill windows) and written into PostgreSQL tables (raw schemas).

3. **Aggregation and Load to BigQuery**

   * Aggregation DAG (`agg_google_dag.py`) transforms spend and performance data and loads it into Google BigQuery.

4. **Backfill Support**

   * Special backfill DAGs allow you to repopulate historical data by specifying date ranges.

5. **Reporting**

   * Postgres holds raw, granular records.
   * BigQuery stores aggregated datasets for analysis.
   * Looker dashboards consume BigQuery for real‑time insights.

## 🔒 Secrets and Config

* **Airflow Variables** store:

  * API keys for Unity, Mintegral, AppsFlyer, Google BC
  * `postgresql_secret` for Postgres connection string
* No credentials are kept in the repository.

## 📈 Technologies

* **Apache Airflow** for orchestration
* **SQLAlchemy** for Postgres connectivity
* **Pandas** for data processing
* **Requests** for API calls
* **Google BigQuery** for aggregated analytics storage
* **Looker** for dashboarding

---

Feel free to explore the DAG definitions and scripts to see how each pipeline is constructed. This setup ensures modularity, secure secret management, and scalability across multiple advertising platforms.
