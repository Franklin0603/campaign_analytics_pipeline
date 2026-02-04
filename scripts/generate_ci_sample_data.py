"""
Generate minimal sample data for CI testing
Matches dbt source schema in models/staging/_sources.yml exactly
"""
import csv
from datetime import datetime, timedelta
import random

def generate_sample_data():
    """Create CSV files matching Bronze layer input (before PySpark processing)"""
    
    import os
    os.makedirs('data/raw', exist_ok=True)
    
    # Generate 10 sample campaigns
    # These will go through Bronze -> Silver transformation via PySpark
    campaigns = []
    campaign_types = ["Search", "Display", "Video", "Social", "Shopping"]
    statuses = ["Active", "Paused", "Completed", "Draft"]
    objectives = ["Traffic", "Engagement", "Sales", "Leads", "Brand Awareness"]
    
    for i in range(1, 11):
        campaigns.append({
            'campaign_id': str(i),
            'advertiser_id': str((i % 3) + 1),
            'campaign_name': f'Campaign {i}',            
            'campaign_type': random.choice(campaign_types),
            'status': random.choice(statuses),
            'objective': random.choice(objectives),
            'start_date': '2024-01-01',
            'end_date': '2024-12-31',
            'budget_daily': str(random.randint(100, 1000)),
            'budget_total': str(random.randint(5000, 50000))
        })
    
    with open('data/raw/campaigns.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=campaigns[0].keys())
        writer.writeheader()
        writer.writerows(campaigns)
    
    # Generate 3 advertisers (matching dbt source: advertiser_id, advertiser_name, industry, country)
    advertisers = [
        {
            'advertiser_id': '1',
            'advertiser_name': 'TechCorp',
            'industry': 'Technology',
            'country': 'US',
            'account_manager':'West1',
        },
        {
            'advertiser_id': '2',
            'advertiser_name': 'RetailPlus',
            'industry': 'Retail',
            'country': 'UK',
            'account_manager':'West2',
        },
        {
            'advertiser_id': '3',
            'advertiser_name': 'FinanceHub',
            'industry': 'Finance',
            'country': 'CA',
            'account_manager':'West2',
        },
    ]
    
    with open('data/raw/advertisers.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=advertisers[0].keys())
        writer.writeheader()
        writer.writerows(advertisers)
    
    # Generate 50 performance records
    # Matching dbt source: performance_id, campaign_id, date, impressions, clicks, conversions, cost, revenue
    performance = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(50):
        impressions = random.randint(1000, 10000)
        clicks = random.randint(50, min(500, int(impressions * 0.1)))  # CTR < 10%
        conversions = random.randint(1, min(50, int(clicks * 0.2)))  # CVR < 20%
        cost = round(random.uniform(100, 1000), 2)
        revenue = round(random.uniform(cost * 0.8, cost * 2.5), 2)  # Variable ROI
        
        performance.append({
            'performance_id': str(i + 1),
            'campaign_id': str(random.randint(1, 10)),
            'date': (base_date + timedelta(days=i % 30)).strftime('%Y-%m-%d'),
            'impressions': str(impressions),
            'clicks': str(clicks),
            'conversions': str(conversions),
            'cost': str(cost),
            'revenue': str(revenue),
        })
    
    with open('data/raw/performance.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=performance[0].keys())
        writer.writeheader()
        writer.writerows(performance)
    
    print("✅ Sample data generated successfully!")
    print(f"  - {len(campaigns)} campaigns")
    print(f"  - {len(advertisers)} advertisers")
    print(f"  - {len(performance)} performance records")
    print(f"\n📋 Columns match dbt source schema:")
    print(f"  Campaigns: {', '.join(campaigns[0].keys())}")
    print(f"  Advertisers: {', '.join(advertisers[0].keys())}")
    print(f"  Performance: {', '.join(performance[0].keys())}")

if __name__ == '__main__':
    generate_sample_data()
