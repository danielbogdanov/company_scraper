#!/usr/bin/env python3
"""
Improved Batch Scraper - Process companies in smaller groups with better logging and error handling
"""

import pandas as pd
import os
import subprocess
import time
from datetime import datetime
import csv
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/batch_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def split_companies_into_batches(csv_file, batch_size=5):
    """Split companies CSV into smaller batches"""
    df = pd.read_csv(csv_file, sep=';')
    
    # Skip header and get total companies
    companies = df.iloc[:, :2].dropna()  # Get company name and domain columns
    total_companies = len(companies)
    
    batches = []
    for i in range(0, total_companies, batch_size):
        batch = companies.iloc[i:i+batch_size]
        batches.append(batch)
    
    return batches, total_companies

def create_batch_csv(batch, batch_num):
    """Create temporary CSV for a batch"""
    batch_file = f'data/batch_{batch_num}.csv'
    
    # Write header
    with open(batch_file, 'w') as f:
        f.write('Company;Domain;;;\n')
        
    # Append batch data
    batch.to_csv(batch_file, sep=';', index=False, header=False, mode='a')
    
    # Log batch contents
    logger.info(f"Batch {batch_num} contains companies:")
    for _, row in batch.iterrows():
        logger.info(f"  - {row.iloc[0]} ({row.iloc[1]})")
    
    return batch_file

def run_batch_scraper(batch_file, batch_num):
    """Run scraper on a single batch with improved error handling"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing Batch {batch_num}")
    logger.info(f"{'='*60}")
    
    # Create a modified scraper script for this batch
    script_content = f'''
import pandas as pd
from scrapy.crawler import CrawlerProcess
from company_scraper import CompanyScraper
import logging
import sys

# Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# Override to load specific batch
def load_batch_companies(self):
    self.companies = []
    df = pd.read_csv('{batch_file}', sep=';')
    
    for _, row in df.iterrows():
        if len(row) >= 2 and pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
            company = {{
                'name': str(row.iloc[0]).strip(),
                'domain': str(row.iloc[1]).strip()
            }}
            self.companies.append(company)
    
    self.custom_logger.info(f"Loaded {{len(self.companies)}} companies from batch {batch_num}")
    print(f"BATCH_LOG: Loaded {{len(self.companies)}} companies")
    for c in self.companies:
        print(f"BATCH_LOG: - {{c['name']}} ({{c['domain']}})")

CompanyScraper.load_companies = load_batch_companies

# Run with timeout settings
settings = {{
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'ROBOTSTXT_OBEY': True,
    'DOWNLOAD_DELAY': 1.5,
    'DOWNLOAD_TIMEOUT': 20,
    'CONCURRENT_REQUESTS': 2,
    'RETRY_TIMES': 2,
    'LOG_LEVEL': 'INFO',
    'CLOSESPIDER_TIMEOUT': 300,  # 5 minutes per batch
}}

try:
    process = CrawlerProcess(settings)
    process.crawl(CompanyScraper)
    process.start()
    print("BATCH_LOG: Scraping completed successfully")
except Exception as e:
    print(f"BATCH_LOG: Error during scraping: {{e}}")
    import traceback
    traceback.print_exc()
'''
    
    # Write and run the batch script
    batch_script = f'batch_{batch_num}_scraper.py'
    with open(batch_script, 'w') as f:
        f.write(script_content)
    
    try:
        # Run with timeout and capture output
        result = subprocess.run(
            ['python', batch_script],
            timeout=360,  # 6 minutes max per batch
            capture_output=True,
            text=True
        )
        
        # Log output
        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    if 'BATCH_LOG:' in line:
                        logger.info(f"Batch {batch_num}: {line.replace('BATCH_LOG:', '').strip()}")
                    
        if result.stderr:
            logger.warning(f"Batch {batch_num} stderr: {result.stderr}")
        
        if result.returncode == 0:
            logger.info(f"✅ Batch {batch_num} completed successfully")
            return True
        else:
            logger.error(f"❌ Batch {batch_num} failed with return code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏱️ Batch {batch_num} timed out after 6 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Error in batch {batch_num}: {e}")
        return False
    finally:
        # Clean up
        if os.path.exists(batch_script):
            os.remove(batch_script)

def check_missing_companies(all_companies_file='data/companies.csv'):
    """Check which companies are missing from the results"""
    logger.info("\n🔍 Checking for missing companies...")
    
    # Load all companies
    df_all = pd.read_csv(all_companies_file, sep=';')
    all_companies = set()
    for _, row in df_all.iterrows():
        if pd.notna(row.iloc[0]):
            all_companies.add(str(row.iloc[0]).strip())
    
    # Load processed companies
    processed_companies = set()
    for file in os.listdir('output'):
        if file.startswith('company_data_') and file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('output', file))
                for company in df['company_name']:
                    processed_companies.add(company)
            except Exception as e:
                logger.warning(f"Error reading {file}: {e}")
    
    # Find missing companies
    missing = all_companies - processed_companies
    if missing:
        logger.warning(f"⚠️ Missing {len(missing)} companies:")
        for company in sorted(missing):
            logger.warning(f"  - {company}")
    else:
        logger.info("✅ All companies were processed!")
    
    return missing

def merge_results():
    """Merge all CSV results into one final file"""
    logger.info("\n🔄 Merging results...")
    
    # Find all company_data CSV files
    output_files = []
    for file in os.listdir('output'):
        if file.startswith('company_data_') and file.endswith('.csv'):
            output_files.append(os.path.join('output', file))
    
    if not output_files:
        logger.error("❌ No output files found")
        return None
    
    # Sort by creation time to get the most recent ones
    output_files.sort(key=os.path.getmtime, reverse=True)
    
    # Merge all results
    all_results = []
    seen_companies = set()
    
    for file in output_files:
        try:
            df = pd.read_csv(file)
            logger.info(f"Reading {file}: {len(df)} companies")
            for _, row in df.iterrows():
                company_key = (row['company_name'], row['domain'])
                if company_key not in seen_companies:
                    all_results.append(row.to_dict())
                    seen_companies.add(company_key)
        except Exception as e:
            logger.error(f"Error reading {file}: {e}")
    
    # Create final merged file
    if all_results:
        final_file = f"output/final_company_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_final = pd.DataFrame(all_results)
        df_final.to_csv(final_file, index=False)
        logger.info(f"✅ Merged {len(all_results)} unique companies into {final_file}")
        return final_file
    else:
        logger.error("❌ No results to merge")
        return None

def main():
    """Main batch processing function"""
    logger.info("🚀 Starting Improved Batch Company Scraper")
    logger.info("=" * 60)
    
    # Split companies into batches
    batches, total = split_companies_into_batches('data/companies.csv', batch_size=5)
    logger.info(f"📊 Total companies: {total}")
    logger.info(f"📦 Number of batches: {len(batches)}")
    
    # Process each batch
    successful_batches = 0
    failed_batches = []
    
    for i, batch in enumerate(batches, 1):
        batch_file = create_batch_csv(batch, i)
        
        if run_batch_scraper(batch_file, i):
            successful_batches += 1
        else:
            failed_batches.append(i)
        
        # Clean up batch file
        if os.path.exists(batch_file):
            os.remove(batch_file)
        
        # Small delay between batches
        if i < len(batches):
            logger.info("⏳ Waiting 5 seconds before next batch...")
            time.sleep(5)
    
    logger.info(f"\n📈 Completed {successful_batches}/{len(batches)} batches successfully")
    if failed_batches:
        logger.warning(f"❌ Failed batches: {failed_batches}")
    
    # Check for missing companies
    missing_companies = check_missing_companies()
    
    # Merge all results
    final_file = merge_results()
    
    if final_file:
        # Show summary
        df = pd.read_csv(final_file)
        logger.info(f"\n📊 Final Results Summary:")
        logger.info(f"   Total companies processed: {len(df)}")
        logger.info(f"   Companies with employee data: {df['employee_count'].notna().sum()}")
        logger.info(f"   Companies with errors: {df['status'].eq('error').sum()}")
        logger.info(f"\n✅ Batch processing completed! Results saved to: {final_file}")
        
        if missing_companies:
            logger.warning(f"\n⚠️ Note: {len(missing_companies)} companies were not processed successfully")
    else:
        logger.error("\n❌ Batch processing completed with errors")

if __name__ == '__main__':
    main()