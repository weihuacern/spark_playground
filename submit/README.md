# Compute Engine Scheduler

## Apache Airflow

```bash
airflow initdb
airflow list_dags
airflow backfill compute_engine_application_scheduler -s 2020-03-18
airflow scheduler
airflow trigger_dag compute_engine_application_scheduler
```
