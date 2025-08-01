#!/usr/bin/env python3
"""
Company Data Scraper Runner

This script runs the company scraper to extract employee count, region, and industry
information from company websites with translation support for non-English content.

Usage:
    python run_scraper.py [options]

Options:
    --companies-file    Path to companies CSV file (default: data/companies.csv)
    --max-companies     Maximum number of companies to process (default: all)
    --output-dir        Output directory for results (default: output/)
    --log-level         Logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO)
    --help              Show this help message

Example:
    python run_scraper.py --max-companies 10 --log-level DEBUG
"""

import argparse
import sys
import os
import logging
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from company_scraper import CompanyScraper


def setup_logging(log_level: str = 'INFO'):
    """Setup logging configuration"""
    log_dir = 'output'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


def validate_files():
    """Validate that required data files exist"""
    required_files = [
        'data/companies.csv',
        'data/industry.csv',
        'data/regions.csv',
        'data/headcount.csv',
        'data/size.csv'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing required data files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        sys.exit(1)
    
    print("✅ All required data files found")


def create_scrapy_settings():
    """Create Scrapy settings for the crawler"""
    settings = {
        'USER_AGENT': 'CompanyScraper (+http://www.yourdomain.com)',
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DEPTH_LIMIT': 2,
        'CLOSESPIDER_TIMEOUT': 600,  # 10 minutes
        'LOG_LEVEL': 'INFO',
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,  # 1 hour cache
        'HTTPCACHE_DIR': 'output/.scrapy_cache',
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 2.0,
        'AUTOTHROTTLE_DEBUG': False,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
    }
    
    return settings


def print_banner():
    """Print application banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                    Company Data Scraper                      ║
    ║                                                              ║
    ║  Extracts employee count, region, and industry information  ║
    ║  from company websites with translation support             ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_summary():
    """Print summary of what the scraper will do"""
    summary = """
    📋 Scraper Configuration:
    
    🎯 Data Sources:
       • Companies: data/companies.csv
       • Industries: data/industry.csv  
       • Regions: data/regions.csv
       • Headcount categories: data/headcount.csv
       • Size categories: data/size.csv
    
    🔍 Extraction Features:
       • Employee count detection with regex patterns
       • Region detection from domain and content
       • Industry classification using keyword matching
       • English version detection and fallback translation
       • Comprehensive logging with reasoning
    
    📁 Output:
       • CSV results in output/ directory
       • Detailed logs with categorization reasoning
       • Timestamped files for multiple runs
    
    🌐 Translation Support:
       • Automatic language detection
       • Translation of non-English content
       • Prioritizes English versions when available
    """
    print(summary)


def main():
    """Main function to run the scraper"""
    parser = argparse.ArgumentParser(
        description='Company Data Scraper - Extract employee count, region, and industry information',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--companies-file',
        default='data/companies.csv',
        help='Path to companies CSV file (default: data/companies.csv)'
    )
    
    parser.add_argument(
        '--max-companies',
        type=int,
        help='Maximum number of companies to process (default: all)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Output directory for results (default: output/)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Print banner and summary
    print_banner()
    print_summary()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Validate required files
    validate_files()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Check companies file
    if not os.path.exists(args.companies_file):
        logger.error(f"Companies file not found: {args.companies_file}")
        sys.exit(1)
    
    logger.info("🚀 Starting Company Data Scraper")
    logger.info(f"📂 Companies file: {args.companies_file}")
    logger.info(f"📁 Output directory: {args.output_dir}")
    logger.info(f"📊 Log level: {args.log_level}")
    
    if args.max_companies:
        logger.info(f"🔢 Max companies: {args.max_companies}")
    
    # Create Scrapy settings
    settings = create_scrapy_settings()
    settings['LOG_LEVEL'] = args.log_level
    
    # Create and configure the crawler process
    process = CrawlerProcess(settings)
    
    # Add custom settings to spider
    spider_kwargs = {}
    if args.max_companies:
        spider_kwargs['max_companies'] = args.max_companies
    
    try:
        # Start the crawler
        process.crawl(CompanyScraper, **spider_kwargs)
        process.start()
        
        logger.info("✅ Scraping completed successfully")
        print("\n🎉 Scraping completed! Check the output/ directory for results.")
        
    except KeyboardInterrupt:
        logger.warning("❌ Scraping interrupted by user")
        print("\n⚠️  Scraping interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ Scraping failed: {e}")
        print(f"\n💥 Scraping failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()