# Airflow Integration Requirements

## Current State

**Complete:**
- ✅ Delta Lake lakehouse (Bronze → Silver → Gold)
- ✅ MinIO object storage
- ✅ Master pipeline script (`run_full_pipeline.py`)
- ✅ dbt transformations
- ✅ Data quality tests
- ✅ Docker Compose infrastructure

**Ready for orchestration:**
- All pipeline scripts are idempotent (can be re-run safely)
- Error handling implemented
- Postgres + MinIO containerized

## Airflow Requirements

### 1. Add Airflow to `docker-compose.yml`

Need to add:
- Airflow webserver
- Airflow scheduler
- Airflow database (Postgres)

### 2. Create DAG

DAG structure:
```
bronze_campaigns → silver_campaigns ─┐
bronze_performance → silver_performance ─┤→ dbt_run → dbt_test
bronze_advertisers → silver_advertisers ─┘
```

### 3. Configuration Needed

- Schedule: Daily at 2 AM
- Retries: 3 attempts with exponential backoff
- SLA: 4 hours
- Email alerts on failure
- Dependencies properly set

### 4. Scripts to Orchestrate

**Bronze layer:**
```bash
python pipeline/bronze/ingest_campaigns.py
python pipeline/bronze/ingest_performance.py
python pipeline/bronze/ingest_advertisers.py
```

**Silver layer:**
```bash
python pipeline/silver/clean_campaigns.py
python pipeline/silver/clean_performance.py
python pipeline/silver/clean_advertisers.py
```

**Gold layer:**
```bash
cd dbt_project && dbt run && dbt test
```

### 5. Monitoring Requirements

- Track pipeline duration
- Track record counts
- Alert on failures
- Dashboard for pipeline health

## Next Session Goals

1. Add Airflow to docker-compose.yml
2. Create campaign_analytics DAG
3. Test full orchestrated pipeline
4. Set up alerts
5. Create monitoring dashboard

## References

- Current pipeline: `run_full_pipeline.py`
- Docker setup: `docker-compose.yml`
- Documentation: `docs/`