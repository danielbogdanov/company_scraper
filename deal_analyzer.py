import pandas as pd
import numpy as np

def analyze_deal_patterns():
    # Load historical deals
    deals_df = pd.read_csv('data/deals/deals.csv', delimiter=';')
    
    # Calculate win rates by different factors
    win_rates = {}
    
    # Win rate by industry
    industry_stats = deals_df.groupby('Industry').agg({
        'Stage': lambda x: (x == 'Closed Won').sum() / len(x),
        'Amount': ['mean', 'sum', 'count']
    }).round(3)
    industry_stats.columns = ['win_rate', 'avg_amount', 'total_amount', 'deal_count']
    win_rates['by_industry'] = industry_stats.to_dict()
    
    # Win rate by region
    region_stats = deals_df.groupby('Region').agg({
        'Stage': lambda x: (x == 'Closed Won').sum() / len(x),
        'Amount': ['mean', 'sum', 'count']
    }).round(3)
    region_stats.columns = ['win_rate', 'avg_amount', 'total_amount', 'deal_count']
    win_rates['by_region'] = region_stats.to_dict()
    
    # Win rate by company size
    size_stats = deals_df.groupby('Company Headcount Size').agg({
        'Stage': lambda x: (x == 'Closed Won').sum() / len(x),
        'Amount': ['mean', 'sum', 'count']
    }).round(3)
    size_stats.columns = ['win_rate', 'avg_amount', 'total_amount', 'deal_count']
    win_rates['by_size'] = size_stats.to_dict()
    
    # Win rate by headcount range
    headcount_stats = deals_df.groupby('Company Headcount Range').agg({
        'Stage': lambda x: (x == 'Closed Won').sum() / len(x),
        'Amount': ['mean', 'sum', 'count']
    }).round(3)
    headcount_stats.columns = ['win_rate', 'avg_amount', 'total_amount', 'deal_count']
    win_rates['by_headcount'] = headcount_stats.to_dict()
    
    return win_rates, deals_df

if __name__ == "__main__":
    patterns, deals = analyze_deal_patterns()
    
    print("\n=== DEAL PATTERN ANALYSIS ===\n")
    
    print("Win Rates by Industry:")
    for industry, stats in patterns['by_industry']['win_rate'].items():
        print(f"  {industry}: {stats*100:.1f}%")
    
    print("\nWin Rates by Region:")
    for region, stats in patterns['by_region']['win_rate'].items():
        print(f"  {region}: {stats*100:.1f}%")
    
    print("\nWin Rates by Company Size:")
    for size, stats in patterns['by_size']['win_rate'].items():
        print(f"  {size}: {stats*100:.1f}%")
    
    print("\nAverage Deal Size by Industry:")
    for industry, amount in patterns['by_industry']['avg_amount'].items():
        print(f"  {industry}: ${amount:.0f}")