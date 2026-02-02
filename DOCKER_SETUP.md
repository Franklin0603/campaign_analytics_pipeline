# 🐳 Docker Setup Guide

## Prerequisites

- Docker Desktop installed ([Download here](https://www.docker.com/products/docker-desktop))
- At least 2GB free disk space

## Quick Start

### 1. Start Database
```bash
# From project root directory
docker compose up -d
```

This will:
- Pull PostgreSQL 15 image (first time only)
- Create database container
- Initialize all schemas (bronze, silver, staging, intermediate, core, analytics, gold, snapshots)
- Make database available on localhost:5432

### 2. Verify Database
```bash
# Check container is running
docker ps

# Should see: campaign_analytics_db

# Test connection
docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics -c "\dn"

# Should list all schemas
```

### 3. Run dbt
```bash
cd dbt_project

# Run models
dbt run

# Run tests
dbt test

# Create snapshots
dbt snapshot
```

## Database Credentials

**For Docker setup:**
- Host: `localhost`
- Port: `5432`
- User: `dbt_user`
- Password: `dbt_password`
- Database: `campaign_analytics`

## Useful Commands

### Start containers
```bash
docker compose up -d
```

### Stop containers
```bash
docker compose down
```

### View logs
```bash
docker compose logs -f postgres
```

### Restart containers
```bash
docker compose restart
```

### Access database directly
```bash
docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics
```

### Remove everything (including data)
```bash
docker compose down -v
```

⚠️ **Warning:** The `-v` flag deletes all data!

## Troubleshooting

### Port 5432 already in use

If you have PostgreSQL running locally:

**Option 1:** Stop local PostgreSQL
```bash
brew services stop postgresql@15
```

**Option 2:** Change Docker port
Edit `docker-compose.yml`:
```yaml
ports:
  - "5433:5432"  # Changed from 5432:5432
```

Then update dbt profiles.yml to use port 5433.

### Container won't start

Check logs:
```bash
docker compose logs postgres
```

### Reset everything
```bash
docker compose down -v
docker compose up -d
```

## Data Persistence

Data is stored in Docker volume `postgres_data`. 

To backup:
```bash
docker exec campaign_analytics_db pg_dump -U dbt_user campaign_analytics > backup.sql
```

To restore:
```bash
cat backup.sql | docker exec -i campaign_analytics_db psql -U dbt_user -d campaign_analytics
```
