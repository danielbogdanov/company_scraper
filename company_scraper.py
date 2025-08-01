import scrapy
import re
import json
import os
import pandas as pd
import csv
import logging
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
from langdetect import detect
import requests
from deep_translator import GoogleTranslator
import time
from typing import Dict, List, Optional, Tuple


class CompanyScraper(scrapy.Spider):
    """
    A comprehensive company data scraper that extracts employee count, region, and industry
    information from company websites with translation support for non-English content.
    """
    
    name = 'company_scraper'
    
    def __init__(self, max_companies=None, *args, **kwargs):
        super(CompanyScraper, self).__init__(*args, **kwargs)
        
        # Store max companies limit
        self.max_companies = max_companies
        
        # Setup logging
        self.setup_logging()
        
        # Load reference data from CSV files
        self.load_reference_data()
        
        # Load companies to scrape
        self.load_companies()
        
        # Setup industry keywords for detection
        self.setup_industry_keywords()
        
        # Initialize translation service
        self.translator = None
        
        # Results storage
        self.results = []
        
    def setup_logging(self):
        """Configure comprehensive logging for categorization reasoning"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('output/scraper.log'),
                logging.StreamHandler()
            ]
        )
        # Don't override Scrapy's logger property, use a different name
        self.custom_logger = logging.getLogger(__name__)
        
    def load_reference_data(self):
        """Load reference data from CSV files"""
        try:
            # Load industries
            with open('data/industry.csv', 'r', encoding='utf-8') as f:
                self.industries = [line.strip() for line in f if line.strip()]
            
            # Load regions
            with open('data/regions.csv', 'r', encoding='utf-8') as f:
                self.regions = [line.strip() for line in f if line.strip()]
            
            # Load headcount categories
            with open('data/headcount.csv', 'r', encoding='utf-8') as f:
                self.headcount_categories = [line.strip() for line in f if line.strip()]
            
            # Load size categories
            with open('data/size.csv', 'r', encoding='utf-8') as f:
                self.size_categories = [line.strip() for line in f if line.strip()]
                
            self.custom_logger.info(f"Loaded reference data: {len(self.industries)} industries, "
                           f"{len(self.regions)} regions, {len(self.headcount_categories)} headcount categories")
                           
        except Exception as e:
            self.custom_logger.error(f"Error loading reference data: {e}")
            raise
    
    def load_companies(self):
        """Load companies from CSV file"""
        try:
            self.companies = []
            df = pd.read_csv('data/companies.csv', sep=';')
            
            for _, row in df.iterrows():
                if len(row) >= 2 and pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                    company = {
                        'name': str(row.iloc[0]).strip(),
                        'domain': str(row.iloc[1]).strip()
                    }
                    self.companies.append(company)
            
            # Apply max_companies limit if specified
            if self.max_companies and len(self.companies) > self.max_companies:
                self.companies = self.companies[:self.max_companies]
                self.custom_logger.info(f"Limited to {len(self.companies)} companies (max_companies={self.max_companies})")
            else:
                self.custom_logger.info(f"Loaded {len(self.companies)} companies to scrape")
            
        except Exception as e:
            self.custom_logger.error(f"Error loading companies: {e}")
            raise
    
    def setup_industry_keywords(self):
        """Setup industry detection keywords based on reference data"""
        self.industry_keywords = {
            "Business Services": [
                'consulting', 'consultancy', 'advisory', 'management consulting', 'strategy consulting',
                'operations consulting', 'outsourcing', 'business process', 'professional services',
                'corporate services', 'audit', 'auditing', 'accounting', 'bookkeeping', 'tax services',
                'legal services', 'law firm', 'compliance', 'risk management', 'human resources',
                'hr services', 'recruitment', 'staffing', 'headhunting', 'talent acquisition',
                'marketing agency', 'advertising agency', 'pr agency', 'public relations',
                'communications', 'branding agency', 'design agency', 'creative agency'
            ],
            
            "Financial Services (excl. Fintech)": [
                'bank', 'banking', 'commercial bank', 'investment bank', 'private bank',
                'asset management', 'wealth management', 'portfolio management', 'fund management',
                'insurance company', 'life insurance', 'property insurance', 'pension fund',
                'investment fund', 'mutual fund', 'hedge fund', 'private equity',
                'venture capital', 'credit union', 'mortgage lender', 'financial advisor',
                'brokerage', 'securities', 'trading firm', 'capital markets'
            ],
            
            "Healthcare, Pharmaceuticals, & Biotech": [
                'hospital', 'clinic', 'medical center', 'healthcare provider', 'medical practice',
                'pharmaceutical company', 'pharma', 'drug development', 'medicine', 'biotech',
                'biotechnology', 'life sciences', 'medical device', 'diagnostic', 'laboratory',
                'clinical research', 'medical research', 'therapy', 'treatment', 'patient care',
                'dental practice', 'veterinary', 'veterinarian', 'vet clinic', 'animal hospital',
                'animal care', 'pet care', 'animal health', 'companion animal', 'livestock',
                'dierenarts', 'dierenkliniek', 'dierenziekenhuis', 'diergeneeskunde', 'veterinair',
                'wellness center', 'fitness center', 'nutrition'
            ],
            
            "Manufacturing (incl. Food & Drink)": [
                'manufacturing', 'factory', 'production facility', 'industrial', 'machinery',
                'equipment manufacturer', 'automotive', 'aerospace', 'chemical', 'steel',
                'metal', 'textile', 'plastic', 'electronics', 'semiconductor', 'food production',
                'beverage', 'brewery', 'distillery', 'food manufacturer', 'packaging',
                'supply chain', 'logistics'
            ],
            
            "Real Estate and Construction": [
                'real estate', 'property development', 'construction company', 'building',
                'architecture', 'engineering', 'residential development', 'commercial development',
                'industrial development', 'infrastructure', 'contractor', 'renovation',
                'design build', 'planning', 'surveying', 'facilities management', 'property management'
            ],
            
            "Retail (incl. Restaurants)": [
                'retail store', 'shop', 'shopping', 'e-commerce', 'ecommerce', 'marketplace',
                'fashion retailer', 'clothing store', 'apparel', 'beauty store', 'cosmetics',
                'jewelry store', 'furniture store', 'home goods', 'garden center',
                'electronics store', 'consumer goods', 'restaurant', 'cafe', 'bar',
                'hospitality', 'hotel', 'travel agency', 'tourism', 'entertainment venue',
                'online store', 'webshop', 'online shopping', 'discount store', 'chain store',
                'fashion', 'clothing', 'textile', 'garment', 'wear', 'outfit', 'style',
                'kids clothing', 'children wear', 'family fashion', 'affordable fashion',
                'budget clothing', 'value retail', 'discount retail', 'fashion chain',
                'boutique', 'department store', 'supermarket', 'grocery', 'convenience store',
                'drugstore', 'pharmacy retail', 'bookstore', 'sporting goods', 'toy store',
                'pet store', 'hardware store', 'home improvement', 'outlet', 'mall',
                'shopping center', 'retail chain', 'store chain', 'retail network'
            ],
            
            "Software & Internet (incl. Video Games)": [
                'software company', 'software development', 'tech company', 'technology company',
                'it company', 'information technology', 'digital agency', 'web development',
                'app development', 'platform', 'saas company', 'cloud services', 'data analytics',
                'artificial intelligence', 'machine learning', 'cybersecurity', 'security software',
                'blockchain', 'cryptocurrency', 'gaming company', 'video game', 'game development',
                'mobile app', 'startup', 'innovation lab'
            ],
            
            "Transportation and Storage": [
                'transportation company', 'transport', 'logistics company', 'shipping company',
                'delivery service', 'freight', 'cargo', 'warehouse', 'storage facility',
                'distribution center', 'trucking company', 'airline',
                'maritime', 'rail', 'fleet management', 'mobility'
            ],
            
            "Aerospace & Defense": [
                'aerospace', 'aviation industry', 'flight', 'aircraft', 'airplane',
                'aviation professionals', 'aviation services', 'aerospace engineering',
                'flight operations', 'aviation technology', 'air transport',
                'aviation safety', 'flight training', 'pilot', 'aviation consulting',
                'aerospace systems', 'aircraft maintenance', 'aviation management'
            ]
        }
    
    def detect_language(self, text: str) -> Optional[str]:
        """Detect the language of text content"""
        try:
            if len(text.strip()) < 20:
                return None
            
            clean_text = re.sub(r'[^\w\s]', ' ', text)
            clean_text = ' '.join(clean_text.split())
            
            if len(clean_text) > 1000:
                clean_text = clean_text[:1000]
            
            language = detect(clean_text)
            return language
        except Exception as e:
            self.custom_logger.warning(f"Language detection failed: {e}")
            return None
    
    def translate_text(self, text: str, source_lang: str = None, target_lang: str = 'en', max_retries: int = 3) -> str:
        """Translate text using Google Translator with error handling and retries"""
        for attempt in range(max_retries):
            try:
                if not text or len(text.strip()) < 3:
                    return text
                
                # Limit text length for translation
                if len(text) > 500:
                    text = text[:500] + "..."
                
                # Auto-detect source language if not provided
                if not source_lang:
                    source_lang = self.detect_language(text)
                    if not source_lang:
                        source_lang = 'auto'
                
                # Skip translation if already English
                if source_lang == 'en' or source_lang == target_lang:
                    return text
                
                translator = GoogleTranslator(source=source_lang, target=target_lang)
                translated = translator.translate(text)
                
                if translated and len(translated.strip()) > 0:
                    self.custom_logger.info(f"Translated text from {source_lang} to {target_lang}")
                    return translated
                else:
                    raise Exception("Empty translation result")
                
            except Exception as e:
                self.custom_logger.warning(f"Translation attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                    continue
                else:
                    self.custom_logger.warning(f"All translation attempts failed, returning original text")
                    return text
        
        return text
    
    def find_english_version(self, domain: str) -> Optional[str]:
        """Try to find English version of the website"""
        possible_urls = [
            f"https://{domain}/en",
            f"https://{domain}/en/",
            f"https://{domain}/english",
            f"https://{domain}/english/",
            f"https://en.{domain}",
            f"https://www.{domain}/en",
            f"https://www.{domain}/en/",
            f"https://www.{domain}/english",
            f"https://www.{domain}/english/"
        ]
        
        for url in possible_urls:
            try:
                response = requests.head(url, timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    self.custom_logger.info(f"Found English URL for {domain}: {url}")
                    return url
            except Exception:
                continue
        
        return None
    
    def find_working_domain(self, original_domain: str) -> Optional[str]:
        """Try to find a working domain by checking common variations"""
        # Extract base domain and TLD
        parts = original_domain.rsplit('.', 1)
        if len(parts) != 2:
            return None
            
        base_domain, tld = parts
        
        # Common TLD variations to try
        tld_variations = []
        
        # If it's a country-specific TLD, try the generic one
        if tld in ['it', 'de', 'fr', 'es', 'nl', 'pl', 'uk']:
            tld_variations.extend(['.com', '.net', '.org', '.eu'])
        # If it's generic, try country-specific ones
        elif tld in ['com', 'net', 'org']:
            tld_variations.extend(['.it', '.de', '.fr', '.es', '.nl', '.uk', '.eu'])
        
        # Also try with/without www
        domain_variations = []
        for new_tld in tld_variations:
            domain_variations.append(f"{base_domain}{new_tld}")
            domain_variations.append(f"www.{base_domain}{new_tld}")
            
        # Test each variation
        for test_domain in domain_variations:
            try:
                response = requests.head(f"https://{test_domain}", timeout=5, allow_redirects=True)
                if response.status_code == 200:
                    self.custom_logger.info(f"Found working domain variation: {test_domain} (original: {original_domain})")
                    return test_domain
            except Exception:
                continue
                
        return None
    
    def find_about_pages(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Find about-us or company pages with translation support"""
        about_keywords = [
            'about', 'about-us', 'about us', 'company', 'über uns', 'chi siamo',
            'quienes somos', 'qui sommes-nous', 'o nas', 'over ons', 'om oss'
        ]
        
        found_urls = []
        
        # Find links with about-related keywords
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            link_text = link.get_text().lower().strip()
            
            # Check URL for keywords
            for keyword in about_keywords:
                if keyword in href or keyword in link_text:
                    full_url = urljoin(base_url, link['href'])
                    if self.is_same_domain(base_url, full_url):
                        found_urls.append(full_url)
                        break
            
            # Try translating short link texts (limited to reduce API calls)
            if len(link_text) < 15 and len(link_text) > 3 and len(found_urls) < 3:
                try:
                    translated = self.translate_text(link_text)
                    for keyword in ['about', 'company', 'team']:
                        if keyword in translated.lower():
                            full_url = urljoin(base_url, link['href'])
                            if self.is_same_domain(base_url, full_url):
                                found_urls.append(full_url)
                                break
                except Exception:
                    continue
        
        return list(set(found_urls))  # Remove duplicates
    
    def is_same_domain(self, base_url: str, target_url: str) -> bool:
        """Check if target URL is from the same domain"""
        try:
            base_domain = urlparse(base_url).netloc.lower()
            target_domain = urlparse(target_url).netloc.lower()
            
            base_clean = base_domain.replace('www.', '')
            target_clean = target_domain.replace('www.', '')
            
            return base_clean == target_clean or base_clean in target_clean or target_clean in base_clean
        except Exception:
            return False
    
    def extract_employee_count(self, soup: BeautifulSoup, translated_content: str = None) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        """Extract employee count with comprehensive patterns and detailed logging"""
        text_content = translated_content if translated_content else soup.get_text()
        original_text = soup.get_text()
        
        if not text_content:
            return None, None, None
        
        # Clean up common text issues from HTML parsing first
        cleaned_text = self.clean_text_for_employee_detection(text_content.lower())
        
        # Then normalize numbers
        normalized_text = self.normalize_numbers(cleaned_text)
        
        # Comprehensive employee count patterns optimized for translated content
        patterns = [
            # Direct patterns (number + job title/role) - adding colleagues
            r'(\d+)\s*(?:\+)?\s*employees?',
            r'(\d+)\s*people',
            r'(\d+)\s*colleagues?',  # Added colleagues keyword
            r'(\d+)\s*members?',
            r'(\d+)\s*professionals?',
            r'(\d+)\s*specialists?',
            r'(\d+)\s*engineers?',
            r'(\d+)\s*developers?',
            r'(\d+)\s*consultants?',
            r'(\d+)\s*experts?',
            r'(\d+)\s*technicians?',
            r'(\d+)\s*staff',
            r'(\d+)\s*workers?',
            r'(\d+)\s*persons?',
            
            # Common translation patterns (Google Translate outputs)
            r'we\s+(?:are|have|employ)\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?|specialists?|experts?)',
            r'our\s+(?:team|company|organization)\s+(?:of|has|consists\s+of|includes)\s+(\d+)',
            r'employs?\s+(?:over|about|approximately|around)?\s*(\d+)\s+(?:people|employees?|colleagues?|professionals?|staff|workers?)',
            r'workforce\s+of\s+(?:over|about|approximately|around)?\s*(\d+)',
            r'has\s+(?:over|about|approximately|around)?\s*(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?|members?)',
            r'consists?\s+of\s+(?:over|about|approximately|around)?\s*(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            r'comprises?\s+(?:of\s+)?(?:over|about|approximately|around)?\s*(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            r'totals?\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            r'counts?\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            
            # More flexible patterns for translations
            r'(?:we|company|organization|firm|business)\s+(?:\w+\s+){0,5}(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            r'(\d+)\s+(?:dedicated|committed|talented|skilled|experienced)?\s*(?:employees?|colleagues?|professionals?|people|staff|workers?)',
            r'(?:with|have|has)\s+(?:\w+\s+){0,3}(\d+)\s+(?:\w+\s+){0,3}(?:employees?|colleagues?|professionals?|people|staff|workers?)',
            
            # Patterns with descriptive words in between (up to 5 words)
            r'(\d+)\s+(?:\w+\s+){1,5}?employees?',
            r'(\d+)\s+(?:\w+\s+){1,5}?colleagues?',  # Added colleagues
            r'(\d+)\s+(?:\w+\s+){1,5}?professionals?',
            r'(\d+)\s+(?:\w+\s+){1,5}?specialists?',
            r'(\d+)\s+(?:\w+\s+){1,5}?engineers?',
            r'(\d+)\s+(?:\w+\s+){1,5}?developers?',
            r'(\d+)\s+(?:\w+\s+){1,5}?consultants?',
            r'(\d+)\s+(?:\w+\s+){1,5}?experts?',
            r'(\d+)\s+(?:\w+\s+){1,5}?technicians?',
            r'(\d+)\s+(?:\w+\s+){1,5}?people',
            r'(\d+)\s+(?:\w+\s+){1,5}?members?',
            r'(\d+)\s+(?:\w+\s+){1,5}?staff',
            r'(\d+)\s+(?:\w+\s+){1,5}?workers?',
            
            # "Team of" patterns
            r'team\s+of\s+(\d+)',
            r'team\s+of\s+(\d+)\s+(?:\w+\s+){1,3}?(?:employees?|people|colleagues?|professionals?|specialists?|engineers?|developers?|consultants?|experts?)',
            
            # "Staff of" patterns  
            r'staff\s+of\s+(\d+)',
            r'workforce\s+of\s+(\d+)',
            
            # "Over/More than" patterns
            r'over\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff)',
            r'over\s+(\d+)\s+(?:\w+\s+){1,3}?(?:employees?|colleagues?|professionals?|specialists?|engineers?|developers?|consultants?|experts?|people)',
            r'more\s+than\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff)',
            r'more\s+than\s+(\d+)\s+(?:\w+\s+){1,3}?(?:employees?|colleagues?|professionals?|specialists?|engineers?|developers?|consultants?|experts?|people)',
            r'approximately\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff)',
            r'about\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff)',
            r'around\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff)',
            
            # Translation-specific patterns for common translation quirks
            r'tall\s+(\d+)\s+.*?(?:employees?|colleagues?)',  # "We are tall 2500 employees" (ponad → tall/high)
            r'high\s+(\d+)\s+.*?(?:employees?|colleagues?)',  # Alternative translation of "ponad"
            r'are\s+\w+\s+(\d+)\s+.*?(?:employees?|colleagues?)',  # "are [word] 2500 employees"
            r'(\d+)\s+\.{2,}\s*(?:employees?|colleagues?)',  # "2500 ... employees" (dots from ellipsis)
            r'we\s+are\s+\w+\s+(\d+)\s+.*?(?:employees?|colleagues?)',  # "we are [word] 2500 employees"
            
            # Company/organization size patterns
            r'company\s+of\s+(\d+)',
            r'organization\s+of\s+(\d+)',
            r'firm\s+of\s+(\d+)',
            r'business\s+with\s+(\d+)\s+(?:employees?|people|colleagues?|staff)',
            r'employing\s+(\d+)\s+(?:people|employees?|colleagues?|professionals?|staff)',
            
            # Range patterns (take lower bound)
            r'(\d+)[-–]\d+\s+(?:employees?|people|colleagues?|professionals?|staff)',
            r'between\s+(\d+)\s+and\s+\d+\s+(?:employees?|people|colleagues?|professionals?|staff)',
            
            # Scattered text patterns (for text spread across elements like KiK)
            # Look for numbers followed by employee-related words within reasonable distance
            r'(\d+)(?:\s+\w+){0,10}?\s+(?:employees?|colleagues?|professionals?|staff|workers?|people|members?)',
            r'(?:we\s+(?:have|employ|are)|our\s+(?:team|company|organization)\s+(?:has|includes|consists\s+of)|employs?|workforce|staff)(?:\s+\w+){0,15}?\s+(\d+)(?:\s+\w+){0,5}?\s+(?:employees?|colleagues?|professionals?|staff|workers?|people|members?)',
            
            # Patterns for enthusiastic/motivated/dedicated etc. (like "3,000 enthusiastic colleagues")
            r'(\d+)\s+(?:enthusiastic|motivated|dedicated|talented|skilled|experienced|professional|passionate|committed)\s+(?:employees?|colleagues?|professionals?|staff|workers?|people|members?)',
            r'(?:over|about|approximately|around)?\s*(\d+)\s+(?:enthusiastic|motivated|dedicated|talented|skilled|experienced|professional|passionate|committed)\s+(?:employees?|colleagues?|professionals?|staff|workers?|people|members?)',
            
            # German/Dutch/Polish patterns (common in original text before translation)
            r'(\d+)\s+(?:mitarbeiter|medewerkers?|werknemers?|personeel|arbeitnehmer|pracowników|pracownikach|collega\'s?)',
            r'mit\s+(?:über|etwa|rund)?\s*(\d+)\s+(?:mitarbeitern?|mitarbeiterinnen?)',
            r'(?:über|etwa|rund)\s+(\d+)\s+(?:mitarbeiter|medewerkers?|werknemers?)',
            
            # Dutch specific patterns (for IVC Evidensia case) - handle both ' and ' apostrophes
            r'zo\'n\s+(\d+)\s+(?:enthousiaste\s+)?collega[\'\u2019]s?',  # "zo'n 3.000 enthousiaste collega's"
            r'ongeveer\s+(\d+)\s+(?:enthousiaste\s+)?collega[\'\u2019]s?',
            r'circa\s+(\d+)\s+(?:enthousiaste\s+)?collega[\'\u2019]s?',
            r'(\d+)\s+enthousiaste\s+collega[\'\u2019]s?',
            r'met\s+(?:zo\'n|ongeveer|circa)?\s*(\d+)\s+.*?collega[\'\u2019]s?',
            r'met\s+zo\'n\s+(\d+)(?:\s+\w+){0,3}\s+collega[\'\u2019]s?',
            r'(?:met|van)\s+(\d+)\s+(?:\w+\s+){0,3}collega[\'\u2019]s?',
            
            # Polish specific patterns
            r'ponad\s*(\d+)\s*(?:pracowników|pracownikach)',
            r'około\s*(\d+)\s*(?:pracowników|pracownikach)',
            r'(\d+)\s*pracowników',
            r'(\d+)\s*pracownikach',
            r'zatrudnia\s*(\d+)\s*(?:pracowników|osób|ludzi)',
            r'zespół\s*(\d+)\s*(?:pracowników|osób)',
            
            # Handle concatenated words (like "wysocyponad")
            r'(?:\w*ponad|ponad)\s*(\d+)\s*(?:pracowników|pracownikach|employees?|people)',
            r'(?:\w*over|over)\s*(\d+)\s*(?:pracowników|pracownikach|employees?|people)',
            
            # Pattern for "X strong" (like "We are 500 strong")
            r'(?:we\s+are|company\s+is|team\s+is)\s+(\d+)\s+strong',
            r'(\d+)\s+strong\s+(?:team|company|organization)',
            r'(\d+)[-\s]+strong',
            
            # Patterns for common mistranslations
            r'(?:about|around|approximately|nearly|roughly|almost)\s+(\d+)\s+(?:employees?|people|colleagues?|professionals?|staff|workers?)',
            r'(\d+)\s+(?:passionate|engaged|motivated|devoted|loyal)\s+(?:employees?|colleagues?|professionals?|people|staff|workers?)',
            
            # More flexible patterns with longer word gaps for scattered HTML
            r'(\d+)(?:(?:\s+\w+){1,20}?)employees?',
            r'(\d+)(?:(?:\s+\w+){1,20}?)colleagues?',
            r'(\d+)(?:(?:\s+\w+){1,20}?)professionals?',
            
            # Aviation-specific patterns (for Sensus Aero type companies)
            r'(?:supported\s+by|backed\s+by|powered\s+by)\s+(\d+)\s+(?:\w+\s+){0,3}(?:aviation|aerospace|flight)\s+professionals?',
            r'(\d+)\s+(?:highly\s+skilled|skilled|experienced|professional)\s+(?:aviation|aerospace|flight)\s+professionals?',
            r'(\d+)\s+(?:aviation|aerospace|flight)\s+professionals?',
            r'(\d+)\s+professionals?\s+(?:in\s+)?(?:aviation|aerospace|flight)',
        ]
        
        # Track all potential matches with their context for logging
        all_matches = []
        
        for pattern in patterns:
            matches = list(re.finditer(pattern, normalized_text))
            for match in matches:
                try:
                    count = int(match.group(1))
                    
                    # Extract context around the match (±50 characters)
                    start = max(0, match.start() - 50)
                    end = min(len(normalized_text), match.end() + 50)
                    context = normalized_text[start:end].strip()
                    
                    # Also get original context if we have translated content
                    original_context = None
                    if translated_content and original_text:
                        # Try to find corresponding position in original text (approximate)
                        orig_normalized = self.normalize_numbers(original_text.lower())
                        if len(orig_normalized) > start:
                            orig_start = max(0, start)
                            orig_end = min(len(orig_normalized), end)
                            original_context = orig_normalized[orig_start:orig_end].strip()
                    
                    all_matches.append({
                        'count': count,
                        'pattern': pattern,
                        'context': context,
                        'original_context': original_context,
                        'match_text': match.group(0)
                    })
                except ValueError:
                    continue
        
        # Filter out obvious false positives (years, etc.)
        valid_matches = []
        for match_info in all_matches:
            count = match_info['count']
            context = match_info['context']
            
            # Skip if it's likely a year (1900-2030)
            if 1900 <= count <= 2030:
                self.custom_logger.info(f"Rejected potential employee count {count} - likely a year. Context: '{context[:100]}'")
                continue
                
            # Skip if it's in a context that suggests date/year
            if re.search(r'\b(?:since|established|founded|year|copyright|©)\b.*?' + str(count), context, re.IGNORECASE):
                self.custom_logger.info(f"Rejected potential employee count {count} - year/date context. Context: '{context[:100]}'")
                continue
                
            # Skip if it's not in reasonable employee range
            if not (1 <= count <= 50000):
                self.custom_logger.info(f"Rejected potential employee count {count} - outside reasonable range. Context: '{context[:100]}'")
                continue
            
            # Skip if context suggests customers/satisfaction/consumers rather than employees
            customer_keywords = [
                'satisfied', 'happy', 'pleased', 'customers?', 'consumers?', 'clients?',
                'visitors?', 'users?', 'subscribers?', 'members?', 'followers?',
                'survey', 'reviews?', 'ratings?', 'feedback', 'testimonials?',
                'shoppers?', 'buyers?', 'purchasers?', 'guests?', 'attendees?'
            ]
            
            customer_context_found = False
            for keyword in customer_keywords:
                # Look for customer keywords in the context around the number
                pattern = rf'\b{keyword}\b'
                if re.search(pattern, context, re.IGNORECASE):
                    # Additional check: if it's "satisfied customers" or similar, it's not employees
                    if re.search(rf'{str(count)}\s+(?:\w+\s+){{0,3}}{keyword}', context, re.IGNORECASE) or \
                       re.search(rf'{keyword}(?:\s+\w+){{0,3}}\s+{str(count)}', context, re.IGNORECASE):
                        customer_context_found = True
                        break
            
            if customer_context_found:
                self.custom_logger.info(f"Rejected potential employee count {count} - customer/satisfaction context detected. Context: '{context[:100]}'")
                continue
                
            valid_matches.append(match_info)
        
        if valid_matches:
            # Sort by count (larger numbers are more likely to be accurate)
            valid_matches.sort(key=lambda x: x['count'], reverse=True)
            best_match = valid_matches[0]
            count = best_match['count']
            category = self.categorize_employee_count(count)
            
            # Create detailed reasoning string
            reasoning_parts = [
                f"Employee count {count} detected from pattern: {best_match['pattern']}",
                f"Matched text: '{best_match['match_text']}'",
                f"Context: '{best_match['context'][:150]}'"
            ]
            
            if best_match['original_context']:
                reasoning_parts.append(f"Original text context: '{best_match['original_context'][:150]}'")
            
            if len(valid_matches) > 1:
                reasoning_parts.append(f"Note: {len(valid_matches)} total matches found, selected highest count")
            
            reasoning = "; ".join(reasoning_parts)
            
            self.custom_logger.info(f"✓ Employee count detected: {count} (category: {category})")
            self.custom_logger.info(f"  Reasoning: {reasoning}")
            
            return count, category, reasoning
        
        # Log when no employee count is found
        if all_matches:
            rejected_counts = [m['count'] for m in all_matches]
            self.custom_logger.info(f"✗ No valid employee count found. Rejected candidates: {rejected_counts} (likely years or invalid contexts)")
        else:
            self.custom_logger.info(f"✗ No employee count patterns matched in text")
        
        return None, None, None
    
    def clean_text_for_employee_detection(self, text: str) -> str:
        """Clean text to improve employee count detection"""
        # Replace multiple dots with single space
        text = re.sub(r'\.{2,}', ' ', text)
        
        # Fix common concatenated words
        text = re.sub(r'wysocyponad', 'wysocy ponad', text)
        text = re.sub(r'sklepówlojalni', 'sklepów lojalni', text)
        text = re.sub(r'klientówodwiedza', 'klientów odwiedza', text)
        text = re.sub(r'pracownikówprofil', 'pracowników profil', text)
        
        # More generic pattern for concatenated words with numbers (but preserve multi-digit numbers)
        # Only split if there are letters on both sides of the complete number
        text = re.sub(r'([a-zA-Z]+)(\d+)([a-zA-Z]+)', r'\1 \2 \3', text)
        
        # Normalize multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()

    def normalize_numbers(self, text: str) -> str:
        """Normalize numbers with various formatting (12,000, 10 000, 1'000'000, 3.000) to plain integers"""
        # Pattern to match numbers with various separators (commas, spaces, apostrophes, dots)
        # Matches: 12,000 | 10 000 | 1'000'000 | 12.000 (European format) | 3'000 | 2,500
        number_pattern = r'\b(\d{1,3}(?:[,\s\'.]\d{3})*)\b'
        
        def replace_number(match):
            number_str = match.group(1)
            # Remove all separators (commas, spaces, apostrophes, dots between digits)
            normalized = re.sub(r'[,\s\'.]+', '', number_str)
            return normalized
        
        # Replace formatted numbers with normalized versions
        normalized_text = re.sub(number_pattern, replace_number, text)
        return normalized_text
    
    def categorize_employee_count(self, count: int) -> str:
        """Categorize employee count into predefined ranges"""
        if count <= 9:
            return "1-9"
        elif count <= 20:
            return "10-20"
        elif count <= 50:
            return "21-50"
        elif count <= 100:
            return "51-100"
        elif count <= 200:
            return "101-200"
        elif count <= 500:
            return "201-500"
        elif count <= 1000:
            return "501-1000"
        elif count <= 5000:
            return "1001-5000"
        else:
            return "over 5000"
    
    def extract_region(self, soup: BeautifulSoup, domain: str) -> Tuple[str, List[str]]:
        """Extract region information with domain-based detection"""
        reasoning = []
        
        # Domain-based region mapping (highest priority)
        domain_region_map = {
            '.nl': 'BeNeLux', '.be': 'BeNeLux', '.lu': 'BeNeLux',
            '.de': 'DACH', '.at': 'DACH', '.ch': 'DACH',
            '.es': 'ES',
            '.fr': 'FR',
            '.uk': 'UKI', '.ie': 'UKI',
            '.pl': 'EU'
        }
        
        # Check domain extension first
        for ext, region in domain_region_map.items():
            if domain.endswith(ext):
                reasoning.append(f"Domain extension {ext} indicates {region}")
                self.custom_logger.info(f"Region detected from domain: {region} (extension: {ext})")
                return region, reasoning
        
        # Country mention mapping
        country_region_map = {
            'netherlands': 'BeNeLux', 'nederland': 'BeNeLux', 'holland': 'BeNeLux',
            'belgium': 'BeNeLux', 'belgië': 'BeNeLux', 'belgique': 'BeNeLux',
            'luxembourg': 'BeNeLux',
            'germany': 'DACH', 'deutschland': 'DACH', 'german': 'DACH',
            'austria': 'DACH', 'österreich': 'DACH',
            'switzerland': 'DACH', 'schweiz': 'DACH', 'suisse': 'DACH',
            'spain': 'ES', 'españa': 'ES', 'spanish': 'ES',
            'france': 'FR', 'français': 'FR', 'french': 'FR',
            'united kingdom': 'UKI', 'uk': 'UKI', 'britain': 'UKI', 'england': 'UKI',
            'ireland': 'UKI', 'irish': 'UKI'
        }
        
        # Check text content for country mentions
        text_content = soup.get_text().lower()
        for country, region in country_region_map.items():
            if country in text_content:
                reasoning.append(f"Country mention: {country}")
                self.custom_logger.info(f"Region detected from text: {region} (country: {country})")
                return region, reasoning
        
        # Default to EU
        reasoning.append("No specific region detected, defaulting to EU")
        self.custom_logger.info("Region defaulted to EU")
        return 'EU', reasoning
    
    def extract_industry(self, soup: BeautifulSoup, company_name: str, translated_content: str = None) -> Tuple[str, List[str]]:
        """Extract industry information with translation support"""
        reasoning = []
        
        # Prepare text sources with weights
        text_sources = [
            {'text': company_name.lower(), 'weight': 3, 'source': 'company_name'},
            {'text': soup.get_text().lower(), 'weight': 1, 'source': 'original_content'}
        ]
        
        if translated_content:
            text_sources.insert(0, {
                'text': translated_content.lower(), 
                'weight': 4, 
                'source': 'translated_content'
            })
        
        # Score each industry
        industry_scores = {}
        
        for industry, keywords in self.industry_keywords.items():
            score = 0
            matched_keywords = []
            
            for source_data in text_sources:
                text = source_data['text']
                weight = source_data['weight']
                source = source_data['source']
                
                for keyword in keywords:
                    if len(keyword) >= 3:
                        pattern = r'\b' + re.escape(keyword) + r'\b'
                        matches = len(re.findall(pattern, text, re.IGNORECASE))
                        if matches > 0:
                            weighted_score = matches * weight
                            score += weighted_score
                            if keyword not in matched_keywords:
                                matched_keywords.append(keyword)
            
            if score > 0:
                industry_scores[industry] = {
                    'score': score,
                    'keywords': matched_keywords
                }
        
        # Select best industry
        if industry_scores:
            sorted_industries = sorted(industry_scores.items(), key=lambda x: x[1]['score'], reverse=True)
            best_industry, best_data = sorted_industries[0]
            best_score = best_data['score']
            
            # Check if there's a clear winner or multiple close matches
            if len(sorted_industries) > 1:
                second_best_score = sorted_industries[1][1]['score']
                # If scores are close, prefer the one with more diverse keywords
                if best_score > 0 and second_best_score / best_score > 0.8:
                    # Both are strong candidates
                    best_keyword_diversity = len(set(best_data['keywords']))
                    second_keyword_diversity = len(set(sorted_industries[1][1]['keywords']))
                    if second_keyword_diversity > best_keyword_diversity:
                        best_industry, best_data = sorted_industries[1]
                        best_score = sorted_industries[1][1]['score']
            
            if best_score >= 1:  # Lowered threshold for better detection
                reasoning.append(f"Keywords matched: {', '.join(list(set(best_data['keywords']))[:5])}")
                reasoning.append(f"Confidence score: {best_score:.1f}")
                self.custom_logger.info(f"Industry detected: {best_industry} (score: {best_score:.1f})")
                return best_industry, reasoning
        
        # Fallback to Unknown
        reasoning.append("No clear industry indicators found")
        self.custom_logger.info("Industry defaulted to Unknown")
        return "Unknown", reasoning
    
    def determine_size_category(self, employee_range: str) -> str:
        """Determine company size category based on employee count range"""
        if not employee_range:
            return "Unknown"
        
        small_ranges = ["1-9", "10-20", "10-50", "21-50"]
        mid_ranges = ["51-100", "101-200"]
        large_ranges = ["201-500", "501-1000"]
        enterprise_ranges = ["1001-5000", "over 5000"]
        
        if employee_range in small_ranges:
            return "Very Small Business"
        elif employee_range in mid_ranges:
            return "Small Business"
        elif employee_range in large_ranges:
            return "Mid-Market"
        elif employee_range in enterprise_ranges:
            return "Enterprise"
        else:
            return "Unknown"
    
    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,  # Reduced from 2 to 1.5
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 3,  # Increased from 2 to 3
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': True,
        'CLOSESPIDER_TIMEOUT': 600,  # Increased to 10 minutes for more companies
        'DEPTH_LIMIT': 2,
        'DOWNLOAD_TIMEOUT': 20,  # Reduced from 30 to 20 seconds
        'RETRY_TIMES': 1,  # Reduced from 2 to 1 for faster processing
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 2.0,
        'AUTOTHROTTLE_DEBUG': False,
    }
    
    def start_requests(self):
        """Generate initial requests for all companies"""
        self.custom_logger.info(f"Starting requests for {len(self.companies)} companies")
        
        for company in self.companies:
            # Try English version first
            english_url = self.find_english_version(company['domain'])
            url = english_url if english_url else f"https://{company['domain']}"
            
            # Store original domain for fallback
            company_meta = company.copy()
            company_meta['original_domain'] = company['domain']
            company_meta['tried_english'] = bool(english_url)
            company_meta['domain_variations_tried'] = False
            
            yield scrapy.Request(
                url=url,
                callback=self.parse_company,
                meta={'company_info': company_meta},
                dont_filter=True,
                errback=self.handle_error
            )
    
    def handle_error(self, failure):
        """Handle request errors"""
        company_info = failure.request.meta.get('company_info', {})
        self.custom_logger.error(f"Request failed for {company_info.get('name', 'Unknown')}: {failure}")
        
        # Check if we should try domain variations
        if not company_info.get('domain_variations_tried', False):
            # Try to find a working domain variation
            original_domain = company_info.get('original_domain', company_info.get('domain'))
            if original_domain:
                working_domain = self.find_working_domain(original_domain)
                if working_domain:
                    self.custom_logger.info(f"Retrying with domain variation: {working_domain}")
                    
                    # Update company info
                    company_info['domain'] = working_domain
                    company_info['domain_variations_tried'] = True
                    
                    # Try with the new domain
                    english_url = self.find_english_version(working_domain)
                    url = english_url if english_url else f"https://{working_domain}"
                    
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_company,
                        meta={'company_info': company_info},
                        dont_filter=True,
                        errback=self.handle_error
                    )
                    return
        
        # Store error result if all attempts failed
        result = {
            'company_name': company_info.get('name', 'Unknown'),
            'domain': company_info.get('original_domain', company_info.get('domain', 'Unknown')),
            'url': failure.request.url,
            'status': 'error',
            'error': str(failure.value),
            'employee_count': None,
            'employee_count_range': None,
            'region': 'EU',
            'industry': 'Unknown',
            'size_category': 'Unknown',
            'reasoning': [f'Error during scraping: {str(failure.value)}'],
            'scraped_at': datetime.now().isoformat()
        }
        
        self.results.append(result)
        yield result
    
    def parse_company(self, response):
        """Main parsing method for company websites"""
        company_info = response.meta['company_info']
        
        self.custom_logger.info(f"Processing {company_info['name']} - {response.url}")
        
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if translation is needed
            text_content = soup.get_text()
            detected_lang = self.detect_language(text_content)
            needs_translation = detected_lang and detected_lang != 'en'
            
            translated_content = None
            if needs_translation:
                self.custom_logger.info(f"Translating content from {detected_lang} to English")
                # Translate key content sections
                try:
                    key_text = self.extract_key_content(soup)
                    if key_text and len(key_text.strip()) > 10:
                        translated_content = self.translate_text(key_text, detected_lang)
                        if not translated_content or len(translated_content.strip()) < 10:
                            self.custom_logger.warning("Translation failed or returned empty content")
                            translated_content = None
                except Exception as e:
                    self.custom_logger.error(f"Translation error for {company_info['name']}: {e}")
                    translated_content = None
            
            # Extract employee count (try translated first, then original if no success)
            employee_count, employee_range, employee_reasoning = self.extract_employee_count(soup, translated_content)
            
            # If no employee count found in translated content, try original text
            if not employee_count and translated_content:
                self.custom_logger.info("No employee count in translated content, trying original text...")
                orig_count, orig_range, orig_reasoning = self.extract_employee_count(soup, None)
                if orig_count:
                    employee_count, employee_range, employee_reasoning = orig_count, orig_range, orig_reasoning
                    employee_reasoning = f"From original text: {employee_reasoning}"
            
            # Extract region
            region, region_reasoning = self.extract_region(soup, company_info['domain'])
            
            # Extract industry
            industry, industry_reasoning = self.extract_industry(soup, company_info['name'], translated_content)
            
            # Determine size category
            size_category = self.determine_size_category(employee_range)
            
            # Compile reasoning
            all_reasoning = []
            if employee_reasoning:
                all_reasoning.append(f"Employee: {employee_reasoning}")
            if region_reasoning:
                all_reasoning.extend([f"Region: {r}" for r in region_reasoning])
            if industry_reasoning:
                all_reasoning.extend([f"Industry: {r}" for r in industry_reasoning])
            
            result = {
                'company_name': company_info['name'],
                'domain': company_info['domain'],
                'url': response.url,
                'status': response.status,
                'detected_language': detected_lang,
                'translated': needs_translation,
                'employee_count': employee_count,
                'employee_count_range': employee_range,
                'region': region,
                'industry': industry,
                'size_category': size_category,
                'reasoning': all_reasoning,
                'scraped_at': datetime.now().isoformat()
            }
            
            self.results.append(result)
            
            # Look for about pages if missing critical info
            if not employee_count or industry == 'Unknown':
                about_pages = self.find_about_pages(soup, response.url)
                if about_pages:
                    for about_url in about_pages[:2]:  # Limit to 2 additional pages
                        yield scrapy.Request(
                            url=about_url,
                            callback=self.parse_about_page,
                            meta={'result': result},
                            dont_filter=True
                        )
                    return
            
            yield result
            
        except Exception as e:
            self.custom_logger.error(f"Error parsing {company_info['name']}: {e}")
            error_result = {
                'company_name': company_info['name'],
                'domain': company_info['domain'],
                'url': response.url,
                'status': 'error',
                'error': str(e),
                'employee_count': None,
                'employee_count_range': None,
                'region': 'EU',
                'industry': 'Unknown',
                'size_category': 'Unknown',
                'reasoning': [f'Error: {str(e)}'],
                'scraped_at': datetime.now().isoformat()
            }
            self.results.append(error_result)
            yield error_result
    
    def extract_key_content(self, soup: BeautifulSoup) -> str:
        """Extract key content sections for translation"""
        key_texts = []
        
        # Title and meta description
        title = soup.find('title')
        if title:
            key_texts.append(title.get_text().strip())
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            key_texts.append(meta_desc['content'].strip())
        
        # Headers
        for heading in soup.find_all(['h1', 'h2', 'h3'])[:10]:
            text = heading.get_text().strip()
            if text and len(text) > 5:
                key_texts.append(text)
        
        # All text elements that might contain employee information
        # Look for any element with numbers
        for elem in soup.find_all(text=re.compile(r'\d+', re.IGNORECASE)):
            parent = elem.parent
            if parent:
                text = parent.get_text().strip()
                # Check if it might be employee-related by looking for context
                if len(text) > 10 and len(text) < 1000:
                    # Add if not already in key_texts
                    if text not in key_texts:
                        key_texts.append(text)
        
        # Limit total content to avoid translation API limits
        combined_text = ' '.join(key_texts)
        if len(combined_text) > 5000:
            combined_text = combined_text[:5000]
        
        return combined_text
    
    def parse_about_page(self, response):
        """Parse additional about/company pages"""
        result = response.meta['result']
        
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Update employee count if not found
            if not result['employee_count']:
                employee_count, employee_range, employee_reasoning = self.extract_employee_count(soup)
                if employee_count:
                    result['employee_count'] = employee_count
                    result['employee_count_range'] = employee_range
                    result['size_category'] = self.determine_size_category(employee_range)
                    if employee_reasoning:
                        result['reasoning'].append(f"About page employee: {employee_reasoning}")
            
            # Update industry if unknown
            if result['industry'] == 'Unknown':
                industry, industry_reasoning = self.extract_industry(soup, result['company_name'])
                if industry != 'Unknown':
                    result['industry'] = industry
                    result['reasoning'].extend([f"About page industry: {r}" for r in industry_reasoning])
            
        except Exception as e:
            self.custom_logger.warning(f"Error parsing about page for {result['company_name']}: {e}")
        
        yield result
    
    def closed(self, reason):
        """Save results to CSV when spider closes"""
        self.custom_logger.info(f"Spider closed: {reason}")
        self.save_results()
    
    def save_results(self):
        """Save all results to CSV file"""
        if not self.results:
            self.custom_logger.warning("No results to save")
            return
        
        output_file = f"output/company_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        fieldnames = [
            'company_name', 'domain', 'url', 'status', 'detected_language', 'translated',
            'employee_count', 'employee_count_range', 'region', 'industry', 'size_category',
            'reasoning', 'scraped_at', 'error'
        ]
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for result in self.results:
                    # Convert reasoning list to string
                    if isinstance(result.get('reasoning'), list):
                        result['reasoning'] = '; '.join(result['reasoning'])
                    writer.writerow(result)
            
            self.custom_logger.info(f"Results saved to {output_file}")
            print(f"✅ Results saved to {output_file}")
            
        except Exception as e:
            self.custom_logger.error(f"Error saving results: {e}")