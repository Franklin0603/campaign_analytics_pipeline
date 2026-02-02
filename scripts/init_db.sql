-- Create schemas for data pipeline layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA silver TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA core TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA gold TO dbt_user;
GRANT ALL PRIVILEGES ON SCHEMA snapshots TO dbt_user;

-- Set search path
ALTER DATABASE campaign_analytics SET search_path TO gold,core,analytics,intermediate,staging,silver,bronze,public;
