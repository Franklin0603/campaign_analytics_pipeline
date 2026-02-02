-- Campaign Analytics Database Setup
-- Run this first: psql campaign_analytics < sql/create_schemas.sql

-- Drop schemas if they exist (for clean start)
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS analytics CASCADE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;

--verify schemas created 
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics')
ORDER BY schema_name;

-- Verify schemas created
SELECT schema_name 
FROM information_schema.schemata
WHERE schema_name IN ('bronze', 'silver', 'gold', 'analytics')
ORDER BY schema_name;

-- Grant permissions to user 
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO postgres;

-- success message
DO $$
BEGIN
    RAISE NOTICE '✅ All schemas created successfully!';
    RAISE NOTICE 'Schemas created and permissions granted successfully';
    RAISE NOTICE 'Schemas: bronze, silver, gold, analytics';
END $$;