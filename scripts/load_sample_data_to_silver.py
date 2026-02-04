"""
Load sample data directly to silver schema for CI testing
Simulates PySpark bronze->silver transformation output
"""
import psycopg2
import csv
from datetime import datetime

def load_to_silver():
    """Load sample CSV data to silver schema"""
    
    # Connect to database
    conn = psycopg2.connect(
        host='localhost',
        user='dbt_user',
        password='dbt_password',
        database='campaign_analytics',
        port=5432
    )
    cur = conn.cursor()
    
    timestamp = datetime.now()
    
    print("📊 Loading sample data to silver schema...")
    
    # Load campaigns
    print("  Loading campaigns...")
    cur.execute("TRUNCATE TABLE silver.campaigns")
    with open('data/raw/campaigns.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO silver.campaigns (
                    campaign_id, campaign_name, advertiser_id, campaign_type,
                    start_date, end_date, budget_daily, budget_total,
                    status, objective, _silver_processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['campaign_id']), row['campaign_name'], int(row['advertiser_id']),
                row['campaign_type'], row['start_date'], row['end_date'],
                float(row['budget_daily']), float(row['budget_total']),
                row['status'], row['objective'], timestamp
            ))
    
    # Load advertisers
    print("  Loading advertisers...")
    cur.execute("TRUNCATE TABLE silver.advertisers")
    with open('data/raw/advertisers.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO silver.advertisers (
                    advertiser_id, advertiser_name, industry, country, account_manager, _silver_processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(row['advertiser_id']), row['advertiser_name'],
                row['industry'], row['country'], row['account_manager'], timestamp
            ))
    
    # Load performance
    print("  Loading performance...")
    cur.execute("TRUNCATE TABLE silver.performance")
    with open('data/raw/performance.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO silver.performance (
                    performance_id, campaign_id, date, impressions, clicks,
                    conversions, cost, revenue, _silver_processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                int(row['performance_id']), int(row['campaign_id']),
                row['date'], int(row['impressions']), int(row['clicks']),
                int(row['conversions']), float(row['cost']),
                float(row['revenue']), timestamp
            ))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ Sample data loaded to silver schema successfully!")

if __name__ == '__main__':
    load_to_silver()
