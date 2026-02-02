"""
Generate realistic sample data for Campaign Analytics Pipeline
Creates: advertisers.csv, campaigns.csv, performance.csv
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Configuration
OUTPUT_DIR = "data/raw"
NUM_ADVERTISERS = 50
NUM_CAMPAIGNS = 200
NUM_PERFORMANCE_RECORDS = 5000

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_advertisers(n=NUM_ADVERTISERS):
    """Generate advertiser dimension data"""
    print(f"📊 Generating {n} advertisers...")
    
    industries = ['Technology', 'Retail', 'Finance', 'Healthcare', 
                  'Education', 'Entertainment', 'Automotive', 'Travel']
    countries = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'BR']
    
    managers = ['Sarah Johnson', 'Mike Chen', 'Emily Rodriguez', 
                'David Kim', 'Lisa Thompson', 'James Wilson']
    
    companies = [
        'TechCorp', 'GlobalRetail', 'FinanceFirst', 'HealthPlus',
        'EduLearn', 'StreamFlix', 'AutoDrive', 'TravelEasy',
        'CloudSystems', 'DataAnalytics', 'SmartHome', 'FitnessPro',
        'FoodDelivery', 'RealEstate', 'InsuranceHub', 'GameZone'
    ]
    
    data = []
    for i in range(1, n + 1):
        data.append({
            'advertiser_id': i,
            'advertiser_name': f"{random.choice(companies)} {i}" if i > len(companies) else random.choice(companies),
            'industry': random.choice(industries),
            'country': random.choice(countries),
            'account_manager': random.choice(managers)
        })
    
    # Add some duplicates and nulls for data quality testing (5%)
    if n > 20:
        # Add 2 duplicate records
        data.append(data[0].copy())
        data.append(data[1].copy())
        
        # Add 1 record with missing industry
        bad_record = data[2].copy()
        bad_record['advertiser_id'] = n + 1
        bad_record['industry'] = None
        data.append(bad_record)
    
    df = pd.DataFrame(data)
    filepath = os.path.join(OUTPUT_DIR, 'advertisers.csv')
    df.to_csv(filepath, index=False)
    print(f"✅ Saved {len(df)} advertisers to {filepath}")
    return df

def generate_campaigns(advertisers_df, n=NUM_CAMPAIGNS):
    """Generate campaign dimension data"""
    print(f"📊 Generating {n} campaigns...")
    
    campaign_types = ['Search', 'Display', 'Video', 'Social', 'Shopping']
    statuses = ['Active', 'Paused', 'Completed', 'Draft']
    objectives = ['Awareness', 'Consideration', 'Conversion', 'Retention']
    
    advertiser_ids = advertisers_df['advertiser_id'].tolist()
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(1, n + 1):
        campaign_start = start_date + timedelta(days=random.randint(0, 300))
        campaign_end = campaign_start + timedelta(days=random.randint(30, 180))
        
        # Random budgets
        daily_budget = round(random.uniform(100, 10000), 2)
        total_budget = round(daily_budget * random.randint(30, 90), 2)
        
        data.append({
            'campaign_id': i,
            'campaign_name': f"Campaign_{random.choice(campaign_types)}_{i}",
            'advertiser_id': random.choice(advertiser_ids),
            'campaign_type': random.choice(campaign_types),
            'start_date': campaign_start.strftime('%Y-%m-%d'),
            'end_date': campaign_end.strftime('%Y-%m-%d'),
            'budget_daily': daily_budget,
            'budget_total': total_budget,
            'status': random.choice(statuses),
            'objective': random.choice(objectives)
        })
    
    # Add data quality issues
    if n > 20:
        # Duplicate campaign
        data.append(data[0].copy())
        
        # Invalid date range (end before start)
        bad_campaign = data[1].copy()
        bad_campaign['campaign_id'] = n + 1
        bad_campaign['start_date'], bad_campaign['end_date'] = bad_campaign['end_date'], bad_campaign['start_date']
        data.append(bad_campaign)
        
        # Missing advertiser_id
        bad_campaign2 = data[2].copy()
        bad_campaign2['campaign_id'] = n + 2
        bad_campaign2['advertiser_id'] = None
        data.append(bad_campaign2)
    
    df = pd.DataFrame(data)
    filepath = os.path.join(OUTPUT_DIR, 'campaigns.csv')
    df.to_csv(filepath, index=False)
    print(f"✅ Saved {len(df)} campaigns to {filepath}")
    return df

def generate_performance(campaigns_df, n=NUM_PERFORMANCE_RECORDS):
    """Generate performance fact data"""
    print(f"📊 Generating {n} performance records...")
    
    campaign_ids = campaigns_df['campaign_id'].tolist()
    
    data = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(1, n + 1):
        # Realistic performance metrics
        impressions = random.randint(1000, 1000000)
        clicks = int(impressions * random.uniform(0.001, 0.05))  # 0.1% to 5% CTR
        conversions = int(clicks * random.uniform(0.01, 0.15))   # 1% to 15% CVR
        
        cost = round(clicks * random.uniform(0.5, 5.0), 2)
        revenue = round(conversions * random.uniform(20, 200), 2)
        
        record_date = start_date + timedelta(days=random.randint(0, 365))
        
        data.append({
            'performance_id': i,
            'campaign_id': random.choice(campaign_ids),
            'date': record_date.strftime('%Y-%m-%d'),
            'impressions': impressions,
            'clicks': clicks,
            'conversions': conversions,
            'cost': cost,
            'revenue': revenue
        })
    
    # Add data quality issues
    if n > 100:
        # Duplicate performance record
        data.append(data[0].copy())
        
        # Negative values
        bad_record = data[1].copy()
        bad_record['performance_id'] = n + 1
        bad_record['cost'] = -100.0
        data.append(bad_record)
        
        # Missing campaign_id
        bad_record2 = data[2].copy()
        bad_record2['performance_id'] = n + 2
        bad_record2['campaign_id'] = None
        data.append(bad_record2)
        
        # Clicks > Impressions (impossible)
        bad_record3 = data[3].copy()
        bad_record3['performance_id'] = n + 3
        bad_record3['clicks'] = bad_record3['impressions'] + 1000
        data.append(bad_record3)
    
    df = pd.DataFrame(data)
    filepath = os.path.join(OUTPUT_DIR, 'performance.csv')
    df.to_csv(filepath, index=False)
    print(f"✅ Saved {len(df)} performance records to {filepath}")
    return df

def main():
    """Generate all sample data files"""
    print("=" * 70)
    print("🎲 GENERATING SAMPLE CAMPAIGN DATA")
    print("=" * 70)
    
    # Generate in proper order (respecting foreign keys)
    advertisers_df = generate_advertisers()
    campaigns_df = generate_campaigns(advertisers_df)
    performance_df = generate_performance(campaigns_df)
    
    print("\n" + "=" * 70)
    print("✅ DATA GENERATION COMPLETE!")
    print("=" * 70)
    print(f"\n📁 Files created in '{OUTPUT_DIR}/':")
    print(f"   - advertisers.csv ({len(advertisers_df)} records)")
    print(f"   - campaigns.csv ({len(campaigns_df)} records)")
    print(f"   - performance.csv ({len(performance_df)} records)")
    print("\n💡 Note: Data includes intentional quality issues for testing:")
    print("   - Duplicates")
    print("   - Null values")
    print("   - Invalid business logic")
    print("   - Constraint violations")
    print("\nReady for pipeline ingestion! 🚀\n")

if __name__ == "__main__":
    main()