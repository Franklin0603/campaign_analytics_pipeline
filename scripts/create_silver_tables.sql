-- Create silver layer tables matching dbt source schema

-- Campaigns table
CREATE TABLE IF NOT EXISTS silver.campaigns (
    campaign_id INTEGER PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    advertiser_id INTEGER NOT NULL,
    campaign_type VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    budget_daily DECIMAL(10, 2),
    budget_total DECIMAL(10, 2),
    status VARCHAR(50),
    objective VARCHAR(100),
    _silver_processed_at TIMESTAMP NOT NULL
);

-- Advertisers table (WITH account_manager)
CREATE TABLE IF NOT EXISTS silver.advertisers (
    advertiser_id INTEGER PRIMARY KEY,
    advertiser_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100) NOT NULL,
    country VARCHAR(10) NOT NULL,
    account_manager VARCHAR(255),
    _silver_processed_at TIMESTAMP NOT NULL
);

-- Performance table
CREATE TABLE IF NOT EXISTS silver.performance (
    performance_id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL,
    date DATE NOT NULL,
    impressions BIGINT NOT NULL,
    clicks BIGINT NOT NULL,
    conversions BIGINT NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    revenue DECIMAL(10, 2) NOT NULL,
    _silver_processed_at TIMESTAMP NOT NULL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_campaigns_advertiser ON silver.campaigns(advertiser_id);
CREATE INDEX IF NOT EXISTS idx_performance_campaign ON silver.performance(campaign_id);
CREATE INDEX IF NOT EXISTS idx_performance_date ON silver.performance(date);
