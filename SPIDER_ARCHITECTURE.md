# Company Spider Architecture Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Current System Architecture](#current-system-architecture)
3. [Spider Initialization & Setup](#spider-initialization--setup)
4. [Web Scraping & Request Handling](#web-scraping--request-handling)
5. [Language Detection & Translation](#language-detection--translation)
6. [Data Extraction Algorithms](#data-extraction-algorithms)
7. [Additional Page Discovery](#additional-page-discovery)
8. [Result Compilation & Storage](#result-compilation--storage)
9. [Kubernetes Deployment Architecture](#kubernetes-deployment-architecture)
10. [Data Flow Journey](#data-flow-journey)
11. [Spider Execution Flow](#spider-execution-flow)

---

## Project Overview

This is a sophisticated web scraping system built with Scrapy that extracts company information (employee count, region, industry) from company websites. The system includes advanced translation capabilities, intelligent page discovery, and a comprehensive analytics dashboard.

### Key Features
- **Multilingual Support**: Detects and translates non-English content
- **Intelligent Page Discovery**: Finds relevant pages (about, team, contact)
- **Advanced Pattern Matching**: 50+ regex patterns for employee count detection
- **Industry Classification**: 8 industry categories with weighted keyword scoring
- **Region Detection**: Domain-based and content-based region identification
- **Analytics Dashboard**: Flask web interface with prospect rating system
- **Batch Processing**: Handles large company lists efficiently

---

## Current System Architecture

### System Overview Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│ data/companies.csv        │ Reference Data CSVs                  │
│ (Company;Domain format)   │ - industry.csv                      │
│                          │ - regions.csv                        │
│                          │ - headcount.csv                      │
│                          │ - size.csv                           │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SPIDER EXECUTION LAYER                       │
├─────────────────────────────────────────────────────────────────┤
│ Entry Points:                                                   │
│ • run_scraper.py (Single run, CLI interface)                   │
│ • batch_scraper.py (Batch processing, 5 companies at a time)   │
│ • company_spider.py (Legacy Scrapy spider)                     │
│ • company_scraper.py (Main enhanced spider class)              │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SCRAPING PIPELINE                          │
├─────────────────────────────────────────────────────────────────┤
│ 1. Domain Resolution & English Detection                        │
│    ├── Try English URLs (/en, /english, en.domain)            │
│    ├── Domain variation fallbacks (.com, .de, etc.)           │
│    └── Working domain detection                                │
│                                                                │
│ 2. Content Extraction & Translation                            │
│    ├── Language detection (langdetect)                        │
│    ├── Translation (Google Translator via deep-translator)     │
│    ├── Key content extraction (titles, headers, paragraphs)    │
│    └── Additional page discovery (about, team, contact)        │
│                                                                │
│ 3. Data Extraction with Regex Patterns                         │
│    ├── Employee Count: 50+ patterns (multilingual)            │
│    ├── Region: Domain + content analysis                       │
│    ├── Industry: Keyword matching with scoring                 │
│    └── Size categorization                                     │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      OUTPUT LAYER                               │
├─────────────────────────────────────────────────────────────────┤
│ output/company_data_YYYYMMDD_HHMMSS.csv                        │
│ ├── company_name, domain, url, status                          │
│ ├── employee_count, employee_count_range                       │
│ ├── region, industry, size_category                            │
│ ├── detected_language, translated, reasoning                   │
│ └── scraped_at, error                                          │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ANALYTICS LAYER                               │
├─────────────────────────────────────────────────────────────────┤
│ company_rater.py (Triggered manually or via web)               │
│ ├── Loads historical deals from data/deals/deals.csv           │
│ ├── Calculates scores based on industry, region, size          │
│ ├── Assigns grades A-D and priority levels                     │
│ └── Outputs: company_ratings_YYYYMMDD_HHMMSS.csv/json         │
│                                                                │
│ analytics_app.py (Flask web dashboard)                         │
│ ├── /prospects - View rated companies with filters             │
│ ├── /export_prospects - CSV export functionality               │
│ ├── /rate_companies - Manual rating trigger                    │
│ └── / - Historical deals dashboard                             │
└─────────────────────────────────────────────────────────────────┘
```

### Spider Workflow Details

**Phase 1: Company Loading**
- Reads `data/companies.csv` (semicolon-separated)
- Loads reference data (industries, regions, headcount ranges)
- Validates input and applies limits if specified

**Phase 2: URL Resolution**
- Attempts to find English versions of websites first
- Falls back to domain variations if original fails
- Uses sophisticated domain matching logic

**Phase 3: Content Processing**
- Detects page language using `langdetect`
- Translates non-English content using Google Translator
- Extracts key content sections for better translation efficiency

**Phase 4: Data Extraction**
- **Employee Count**: 50+ regex patterns covering multiple languages
- **Region**: Domain-based (.de→DACH) + content analysis
- **Industry**: Keyword scoring across 8 categories with weighted sources
- **Size**: Maps employee count to business size categories

**Phase 5: Additional Page Discovery**
- Finds relevant pages (about, team, contact) via link analysis
- Translates navigation text to discover non-English pages
- Limits to 2 additional pages per company to prevent infinite crawling

---

## Spider Initialization & Setup

### Class Definition and Constructor

```python
class CompanySpider(scrapy.Spider):
    name = 'company_scraper'
    
    def __init__(self, companies_file=None, *args, **kwargs):
        super(CompanySpider, self).__init__(*args, **kwargs)
        
        # Store companies file path
        self.companies_file = companies_file
        
        # Load categories from CSV files
        self.load_categories()
        
        # Load companies from CSV
        self.load_companies()
        
        # Load industry keywords mapping
        self.setup_industry_keywords()
```

**What this does:**
- **Inherits from scrapy.Spider**: Gets all Scrapy framework functionality
- **companies_file parameter**: Allows custom company list (defaults to data/companies.csv)
- **Sequential initialization**: Categories → Companies → Industry keywords
- **Memory storage**: All data loaded into instance variables for fast access

### Category Loading (`load_categories()`)

```python
def load_categories(self):
    """Load categories from CSV files"""
    # Default categories (fallback)
    self.HEADCOUNT_RANGES = ["1-9", "10-20", "10-50", "101-200", ...]
    self.REGIONS = ["BeNeLux", "DACH", "ES", "EU", "FR", "UKI"]
    self.SIZE_CATEGORIES = ["Enterprise", "Mid-Market", ...]
    self.INDUSTRIES = ["Business Services", "Financial Services", ...]
    
    # Try to load from files
    file_mapping = {
        'HEADCOUNT_RANGES': ['data/headcount.csv', 'headcount.csv'],
        'REGIONS': ['data/regions.csv', 'regions.csv'],
        'SIZE_CATEGORIES': ['data/size.csv', 'size.csv'],
        'INDUSTRIES': ['data/industry.csv', 'industry.csv']
    }
    
    for attr_name, possible_paths in file_mapping.items():
        for file_path in possible_paths:
            try:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        values = [line.strip() for line in f if line.strip()]
                        setattr(self, attr_name, values)
```

**What this does:**
- **Fallback strategy**: Hard-coded defaults in case files are missing
- **Multiple path attempts**: Tries `data/headcount.csv` then `headcount.csv`
- **Dynamic attribute setting**: `setattr()` assigns loaded data to instance variables
- **Line-by-line reading**: Each line becomes a list item, empty lines skipped
- **Error resilience**: If file loading fails, keeps defaults

### Company Loading (`load_companies()`)

```python
def load_companies(self):
    """Load companies from CSV file"""
    self.COMPANIES = []
    
    # Use provided companies_file parameter first, then fallback to default paths
    possible_paths = []
    if self.companies_file:
        possible_paths.append(self.companies_file)
    possible_paths.extend(['data/companies.csv', 'companies.csv'])
    
    for csv_file in possible_paths:
        try:
            if os.path.exists(csv_file):
                df = pd.read_csv(csv_file, sep=';')
                self.logger.info(f"CSV columns: {df.columns.tolist()}")
                
                for _, row in df.iterrows():
                    if len(row) >= 2 and pd.notna(row.iloc[0]) and pd.notna(row.iloc[1]):
                        company = {
                            'name': str(row.iloc[0]).strip(),
                            'domain': str(row.iloc[1]).strip()
                        }
                        self.COMPANIES.append(company)
```

**What this does:**
- **Priority loading**: Custom file first, then standard paths
- **Pandas parsing**: Uses `;` separator for CSV reading
- **Column inspection**: Logs available columns for debugging
- **Row validation**: Ensures at least 2 columns with non-null values
- **Data extraction**: Takes only first two columns (name, domain)
- **Data cleaning**: `strip()` removes whitespace
- **Dictionary structure**: Each company becomes `{'name': 'X', 'domain': 'Y'}`

### Industry Keywords Setup (`setup_industry_keywords()`)

```python
def setup_industry_keywords(self):
    """Setup industry detection keywords with more precise matching"""
    self.industry_keywords = {
        "Business Services": [
            'consulting', 'consultancy', 'advisory', 'management consulting',
            'operations consulting', 'outsourcing', 'business process',
            'professional services', 'corporate services', 'audit',
            # ... 20+ more keywords
        ],
        
        "Financial Services (excl. Fintech)": [
            'bank', 'banking', 'commercial bank', 'investment bank',
            'asset management', 'wealth management', 'insurance company',
            # ... 15+ more keywords
        ],
        # ... 6 more industries with keywords
    }
    
    # Add industry-specific exclusions to avoid false positives
    self.industry_exclusions = {
        "Software & Internet (incl. Video Games)": [
            'manufacturing software', 'healthcare software', 'financial software',
            'retail software', 'construction software'
        ]
    }
```

**What this does:**
- **Keyword mapping**: Each industry gets list of detection keywords
- **Multilingual support**: Includes terms in German, Dutch, Polish
- **Hierarchical keywords**: From general ("consulting") to specific ("management consulting")
- **Exclusion patterns**: Prevents false positives (e.g., "healthcare software" ≠ Software industry)
- **Industry coverage**: 8 major industry categories with 15-25 keywords each

---

## Web Scraping & Request Handling

### Scrapy Settings Configuration

```python
custom_settings = {
    'DOWNLOAD_DELAY': 1,              # Wait 1 second between requests
    'RANDOMIZE_DOWNLOAD_DELAY': False, # Consistent timing
    'CONCURRENT_REQUESTS': 4,          # Max 4 parallel requests total
    'CONCURRENT_REQUESTS_PER_DOMAIN': 1, # Only 1 request per domain at a time
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...',
    'ROBOTSTXT_OBEY': True,           # Respect robots.txt files
    'CLOSESPIDER_PAGECOUNT': 10,      # Stop after 10 pages total
    'CLOSESPIDER_TIMEOUT': 60,        # Stop after 60 seconds
    'DEPTH_LIMIT': 1,                 # Max 1 level of link following
    'CLOSESPIDER_ITEMCOUNT': 10,      # Stop after 10 items scraped
}
```

**What this does:**
- **Rate limiting**: Prevents overwhelming target servers
- **Politeness**: Respects robots.txt and uses realistic user agent
- **Resource limits**: Prevents infinite crawling or runaway processes
- **Domain isolation**: One request per domain prevents blocking
- **Timeout protection**: Ensures spider doesn't run indefinitely

### English URL Discovery (`find_english_url()`)

```python
def find_english_url(self, domain):
    """Try to find English version of the website"""
    possible_english_urls = [
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
    
    for url in possible_english_urls:
        try:
            response = requests.head(url, timeout=5, allow_redirects=True)
            if response.status_code == 200:
                self.logger.info(f"Found English URL for {domain}: {url}")
                return url
        except Exception as e:
            continue
    
    return None
```

**What this does:**
- **Systematic URL testing**: Tries common English URL patterns
- **HEAD requests**: Faster than GET, only checks if URL exists
- **Timeout protection**: 5-second limit prevents hanging
- **Error handling**: Continues to next URL if one fails
- **Early return**: Returns first working English URL found

### Request Generation (`start_requests()`)

```python
def start_requests(self):
    """Generate initial requests for all company websites"""
    self.logger.info(f"Starting requests for {len(self.COMPANIES)} companies")
    
    for company in self.COMPANIES:
        # Try to find English version first
        english_url = self.find_english_url(company['domain'])
        url = english_url if english_url else f"https://{company['domain']}"
        
        yield scrapy.Request(
            url=url,
            callback=self.parse_company,
            meta={'company_info': company, 'tried_english': bool(english_url)},
            dont_filter=True,
            errback=self.handle_error
        )
```

**What this does:**
- **Iterator pattern**: `yield` creates generator, memory efficient
- **English preference**: Always tries English version first
- **Fallback strategy**: Uses main domain if no English version
- **Metadata passing**: Attaches company info to each request
- **Error handling**: `errback` catches failed requests
- **Filter bypass**: `dont_filter=True` allows duplicate URLs

### Error Handling (`handle_error()`)

```python
def handle_error(self, failure):
    """Handle request errors"""
    company_info = failure.request.meta.get('company_info', {})
    self.logger.error(f"Request failed for {company_info.get('name', 'Unknown')}: {failure}")
    
    # Still yield a result with error information
    yield {
        'name': company_info.get('name', 'Unknown'),
        'domain': company_info.get('domain', 'Unknown'),
        'url': failure.request.url,
        'status': 'error',
        'error': str(failure.value),
        'employee_count': None,
        'employee_count_range': None,
        'region': 'EU',  # Default region
        'industry': 'Unknown',  # Default industry
        'size_category': 'Unknown',
        'location_info': ['Error during scraping'],
        'industry_indicators': ['Error during scraping'],
        'confidence_score': 0,
        'scraped_at': datetime.now().isoformat()
    }
```

**What this does:**
- **Graceful degradation**: Creates partial result even on failure
- **Error logging**: Records failure reason for debugging
- **Default values**: Fills in reasonable defaults for missing data
- **Data consistency**: Maintains same result structure as successful scrapes
- **Timestamp tracking**: Records when error occurred

---

## Language Detection & Translation

### Language Detection (`detect_language()`)

```python
def detect_language(self, text):
    """Detect language of text content"""
    try:
        if len(text.strip()) < 20:  # Too short for reliable detection
            return None
        
        # Clean text for better detection
        clean_text = re.sub(r'[^\w\s]', ' ', text)  # Remove punctuation
        clean_text = ' '.join(clean_text.split())   # Normalize whitespace
        
        if len(clean_text) > 1000:  # Use sample for very long text
            clean_text = clean_text[:1000]
        
        language = detect(clean_text)
        return language
    except Exception as e:
        self.logger.warning(f"Language detection failed: {e}")
        return None
```

**What this does:**
- **Length validation**: Needs minimum 20 characters for accuracy
- **Text cleaning**: Removes punctuation that confuses language detection
- **Whitespace normalization**: Collapses multiple spaces to single space
- **Length limiting**: Uses first 1000 chars for performance
- **Error resilience**: Returns None if detection fails
- **Uses langdetect library**: Statistical language identification

### Translation Need Assessment (`needs_translation()`)

```python
def needs_translation(self, soup, threshold=0.7):
    """Check if page content needs translation to English"""
    try:
        # Get text from multiple sources for better language detection
        text_sources = []
        
        # Get title and meta description (more reliable for language detection)
        title = soup.find('title')
        if title:
            text_sources.append(title.get_text())
        
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            text_sources.append(meta_desc.get('content'))
        
        # Get text from main content areas
        main_content = soup.find_all(['p', 'div', 'span'], limit=10)
        for element in main_content:
            text = element.get_text().strip()
            if len(text) > 30:  # Only substantial text
                text_sources.append(text[:200])
        
        # Combine all text sources
        combined_text = ' '.join(text_sources)
        if len(combined_text) < 30:  # Fallback to all body text
            combined_text = soup.get_text()[:1000]
        
        language = self.detect_language(combined_text)
        
        # Conservative approach - only translate if confident it's non-English
        if language and language != 'en':
            return True, language
        else:
            return False, 'en'
            
    except Exception as e:
        return False, 'en'
```

**What this does:**
- **Multi-source sampling**: Uses title, meta description, and content paragraphs
- **Reliability prioritization**: Title and meta are most reliable for language detection
- **Length filtering**: Only uses substantial text (>30 chars)
- **Fallback strategy**: Uses full body text if structured content insufficient
- **Conservative approach**: Assumes English on uncertainty to avoid unnecessary translation

### Text Translation (`translate_text()`)

```python
def translate_text(self, text, source_lang=None, target_lang='en'):
    """Translate text using Google Translate with timeout protection"""
    try:
        if not text or len(text.strip()) < 3:
            return text
        
        # Limit text length for translation (API limits)
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
        
        # Add timeout protection
        import signal
        def timeout_handler(signum, frame):
            raise TimeoutError("Translation timeout")
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(5)  # 5 second timeout
        
        try:
            translator = GoogleTranslator(source=source_lang, target=target_lang)
            translated = translator.translate(text)
            return translated
        finally:
            signal.alarm(0)  # Cancel timeout
        
    except (TimeoutError, Exception) as e:
        self.logger.warning(f"Translation failed: {e}")
        return text  # Return original text on failure
```

**What this does:**
- **Input validation**: Skips very short or empty text
- **Length limiting**: Truncates to 500 chars to avoid API limits
- **Language auto-detection**: Detects source language if not provided
- **Skip unnecessary translation**: Returns original if already English
- **Timeout protection**: Uses Unix signals to prevent hanging
- **Graceful failure**: Returns original text if translation fails

### CSS/Code Detection (`is_css_or_code()`)

```python
def is_css_or_code(self, text):
    """Check if text looks like CSS, JavaScript, or other code"""
    if not text or len(text) < 10:
        return False
        
    # Common CSS/JS patterns
    css_patterns = [
        r'{[^}]*}',  # CSS rules
        r'--[\w-]+:',  # CSS variables
        r'@media',  # CSS media queries
        r'function\s*\(',  # JavaScript functions
        r'<[^>]+>',  # HTML tags
    ]
    
    # Count CSS/code-like patterns
    pattern_count = 0
    for pattern in css_patterns:
        if re.search(pattern, text):
            pattern_count += 1
    
    # If more than 2 patterns match, likely CSS/code
    if pattern_count >= 2:
        return True
        
    # Additional checks for CSS-specific content
    css_keywords = ['border-radius', 'background-color', 'margin', 'padding']
    css_keyword_count = sum(1 for keyword in css_keywords if keyword in text.lower())
    
    return css_keyword_count >= 3
```

**What this does:**
- **Pattern matching**: Uses regex to identify CSS/JS/HTML patterns
- **Scoring system**: Counts how many code patterns are found
- **Threshold detection**: 2+ patterns = likely code
- **CSS keyword detection**: Identifies CSS properties
- **Prevents translation pollution**: Avoids translating technical content

---

## Data Extraction Algorithms

### Employee Count Extraction - The Core Algorithm

This is the most complex part of the spider, using sophisticated pattern matching:

#### Phase 1: Text Preprocessing

```python
def extract_employee_count(self, soup, raw_text):
    """Extract employee count with comprehensive patterns"""
    result = {'employee_count': None, 'employee_count_range': None}
    
    # Extract clean text from soup
    clean_text = soup.get_text() if soup else raw_text
    
    # Early exit if no meaningful content
    if not clean_text or len(clean_text.strip()) < 20:
        return result
```

#### Phase 2: Number Normalization

```python
def normalize_numbers(self, text):
    """Normalize numbers with various formatting to plain integers"""
    # Pattern: 12,000 | 10 000 | 1'000'000 | 12.000 (European format)
    number_pattern = r'\b(\d{1,3}(?:[,\s\'.]\d{3})*)\b'
    
    def replace_number(match):
        number_str = match.group(1)
        # Remove all separators
        normalized = re.sub(r'[,\s\'.]+', '', number_str)
        return normalized
    
    # Replace formatted numbers with normalized versions
    normalized_text = re.sub(number_pattern, replace_number, text)
    return normalized_text
```

**Example**: "We have 3,000 employees" → "We have 3000 employees"

#### Phase 3: Pattern Matching (50+ Patterns)

```python
# Comprehensive employee count patterns
employee_patterns = [
    # Direct patterns
    r'(\d+)\s*(?:\+)?\s*employees?',
    r'(\d+)\s*people',
    r'(\d+)\s*colleagues?',
    r'(\d+)\s*professionals?',
    
    # Translation patterns (Google Translate outputs)
    r'we\s+(?:are|have|employ)\s+(\d+)\s+(?:employees?|people|staff)',
    r'employs?\s+(?:over|about)?\s*(\d+)\s+(?:people|employees?)',
    
    # Translation quirks
    r'tall\s+(\d+)\s+.*?employees?',  # "ponad" → "tall/high"
    r'high\s+(\d+)\s+.*?employees?',
    
    # Dutch patterns
    r'zo\'n\s+(\d+)\s+(?:enthousiaste\s+)?collega[\'\u2019]s?',
    
    # Polish patterns
    r'ponad\s*(\d+)\s*(?:pracowników|pracownikach)',
    r'około\s*(\d+)\s*(?:pracowników|pracownikach)',
    
    # Range patterns (take lower bound)
    r'(\d+)[-–]\d+\s+(?:employees?|people)',
    r'between\s+(\d+)\s+and\s+\d+\s+(?:employees?|people)',
]
```

#### Phase 4: False Positive Filtering

```python
# Filter out obvious false positives
valid_matches = []
for match_info in all_matches:
    count = match_info['count']
    context = match_info['context']
    
    # Skip years (1900-2030)
    if 1900 <= count <= 2030:
        continue
        
    # Skip date contexts
    if re.search(r'\b(?:since|established|founded|year)\b.*?' + str(count), context):
        continue
        
    # Skip unreasonable ranges
    if not (1 <= count <= 50000):
        continue
    
    # Skip customer contexts
    customer_keywords = ['satisfied', 'customers', 'clients', 'visitors']
    if any(keyword in context.lower() for keyword in customer_keywords):
        continue
        
    valid_matches.append(match_info)
```

#### Phase 5: Result Selection

```python
if valid_matches:
    # Sort by count (larger numbers more likely accurate)
    valid_matches.sort(key=lambda x: x['count'], reverse=True)
    best_match = valid_matches[0]
    count = best_match['count']
    category = self.categorize_employee_count(count)
    
    return count, category, reasoning
```

### Industry Detection Algorithm

```python
def extract_industry_info(self, soup, raw_text, company_name):
    """Extract industry with weighted keyword scoring"""
    
    # Text sources with different weights
    text_sources = [
        {'text': company_name.lower(), 'weight': 3, 'source': 'company_name'},
        {'text': soup.get_text().lower(), 'weight': 1, 'source': 'body_text'}
    ]
    
    # Prioritize translated content (highest weight)
    translated_div = soup.find('div', class_='translated-content')
    if translated_div:
        text_sources.insert(0, {
            'text': translated_div.get_text().lower(), 
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
            
            for keyword in keywords:
                if len(keyword) >= 3:
                    pattern = r'\b' + re.escape(keyword) + r'\b'
                    matches = len(re.findall(pattern, text, re.IGNORECASE))
                    if matches > 0:
                        weighted_score = matches * weight
                        score += weighted_score
                        matched_keywords.append(keyword)
        
        if score > 0:
            industry_scores[industry] = {
                'score': score,
                'keywords': matched_keywords
            }
    
    # Select highest scoring industry
    if industry_scores:
        best_industry = max(industry_scores.items(), key=lambda x: x[1]['score'])
        if best_industry[1]['score'] >= 2:  # Minimum threshold
            return best_industry[0], matched_keywords
    
    return "Unknown", []
```

### Region Detection Algorithm

```python
def extract_location_info(self, soup, raw_text, domain):
    """Extract region with domain-priority detection"""
    
    # Domain-based region mapping (highest priority)
    domain_region_map = {
        '.nl': 'BeNeLux', '.be': 'BeNeLux', '.lu': 'BeNeLux',
        '.de': 'DACH', '.at': 'DACH', '.ch': 'DACH',
        '.es': 'ES', '.fr': 'FR',
        '.uk': 'UKI', '.ie': 'UKI',
        '.pl': 'EU'
    }
    
    # Check domain extension first (takes precedence)
    for ext, region in domain_region_map.items():
        if domain.endswith(ext):
            return region, [f"Domain extension: {ext} (high confidence)"]
    
    # Fallback to text-based detection
    country_region_map = {
        'netherlands': 'BeNeLux', 'germany': 'DACH', 'spain': 'ES',
        'france': 'FR', 'united kingdom': 'UKI'
    }
    
    text_content = soup.get_text().lower()
    for country, region in country_region_map.items():
        if country in text_content:
            return region, [f"Country mention: {country}"]
    
    # Default to EU
    return 'EU', ["Default: EU (no specific region detected)"]
```

---

## Additional Page Discovery

### Relevant Page Discovery (`find_relevant_pages()`)

This sophisticated system finds additional pages that might contain better company information:

```python
def find_relevant_pages(self, soup, base_url):
    """Find links to pages with employee/company info"""
    
    # Keywords for page discovery
    relevant_keywords = [
        'about', 'team', 'contact', 'company', 'people', 'staff', 
        'careers', 'services', 'who we are', 'our team'
    ]
    
    relevant_urls = []
    collected_links = set()
    
    # First pass: keyword-based discovery
    for link in soup.find_all('a', href=True):
        href = link['href'].lower()
        link_text = link.get_text().lower().strip()
        
        # Skip fragments and external links
        if (link['href'].startswith('#') or 
            link['href'].startswith('mailto:') or 
            link['href'].startswith('tel:')):
            continue
        
        # Check URL for keywords
        matched = False
        for keyword in relevant_keywords:
            if keyword in href:
                full_url = urljoin(base_url, link['href'])
                if (full_url not in collected_links and 
                    self.is_same_domain_or_subdomain(base_url, full_url)):
                    relevant_urls.append(full_url)
                    collected_links.add(full_url)
                    matched = True
                    break
```

### Link Text Translation for Multi-language Sites

```python
        # Translate link text if needed
        if not matched and link_text and len(link_text) <= 20:
            try:
                translated_text = self.translate_text(link_text, target_lang='en')
                if translated_text and translated_text != link_text:
                    # Check translated text against keywords
                    for keyword in relevant_keywords:
                        if keyword in translated_text.lower():
                            full_url = urljoin(base_url, link['href'])
                            if (full_url not in collected_links and 
                                self.is_same_domain_or_subdomain(base_url, full_url)):
                                relevant_urls.append(full_url)
                                collected_links.add(full_url)
                                matched = True
                                break
            except Exception:
                continue
```

**Example**: "O nas" (Polish) → "About us" (English) → matches "about" keyword

### Navigation Menu Discovery

```python
    # Second pass: navigation menu discovery
    nav_elements = soup.find_all(['nav', 'ul', 'ol'], 
                                class_=re.compile(r'(nav|menu|navigation)', re.I))
    for nav in nav_elements:
        for link in nav.find_all('a', href=True):
            if len(relevant_urls) >= 15:
                break
                
            full_url = urljoin(base_url, link['href'])
            if (full_url not in collected_links and 
                self.is_same_domain_or_subdomain(base_url, full_url) and 
                full_url != base_url):
                relevant_urls.append(full_url)
                collected_links.add(full_url)
    
    return relevant_urls[:7]  # Limit to 7 pages
```

### Subdomain Discovery

```python
    # Subdomain root discovery
    subdomain_roots = set()
    for url in relevant_urls:
        parsed = urlparse(url)
        if parsed.hostname:
            hostname_parts = parsed.hostname.split('.')
            if len(hostname_parts) >= 3:  # Has subdomain
                subdomain = hostname_parts[0].lower()
                
                # Check for company subdomains
                english_company_subdomains = ['company', 'about', 'corporate']
                if subdomain in english_company_subdomains:
                    subdomain_root = f"https://{parsed.hostname}/"
                    subdomain_roots.add(subdomain_root)
                
                # Try translating subdomain names
                elif len(subdomain) <= 10 and subdomain.isalpha():
                    try:
                        translated = self.translate_text(subdomain, target_lang='en')
                        if translated.lower() in ['company', 'firm', 'about']:
                            subdomain_root = f"https://{parsed.hostname}/"
                            subdomain_roots.add(subdomain_root)
                    except Exception:
                        pass
    
    # Prioritize subdomain roots
    prioritized_urls = list(subdomain_roots) + relevant_urls
    return prioritized_urls[:7]
```

### Additional Page Processing

```python
def parse_additional_page(self, response):
    """Parse additional pages like About, Team, Contact"""
    result = response.meta['result']
    
    try:
        result['pages_visited'].append(response.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Update missing information only
        if not result['employee_count']:
            employee_info = self.extract_employee_count(soup, response.text)
            if employee_info['employee_count']:
                result.update(employee_info)
        
        if not result['region'] or result['region'] == 'EU':
            location_info = self.extract_location_info(soup, response.text, result['domain'])
            if location_info['region'] != 'EU':
                result.update(location_info)
        
        if not result['industry'] or result['industry'] == 'Unknown':
            industry_info = self.extract_industry_info(soup, response.text, result['name'])
            if industry_info['industry'] != 'Unknown':
                result.update(industry_info)
        
        # Update size category
        result['size_category'] = self.determine_size_category(result['employee_count_range'])
        
    except Exception as e:
        self.logger.warning(f"Error parsing additional page: {e}")
    
    yield result
```

---

## Result Compilation & Storage

### Main Parsing Orchestration

```python
def parse_company(self, response):
    """Main parsing method orchestrating all extraction"""
    company_info = response.meta['company_info']
    
    # Initialize result structure
    result = {
        'name': company_info['name'],
        'domain': company_info['domain'],
        'url': response.url,
        'status': response.status,
        'employee_count': None,
        'employee_count_range': None,
        'region': None,
        'industry': None,
        'size_category': None,
        'location_info': [],
        'industry_indicators': [],
        'confidence_score': 0,
        'scraped_at': datetime.now().isoformat(),
        'pages_visited': [response.url]
    }
    
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Language detection and translation
        needs_trans, detected_lang = self.needs_translation(soup)
        
        # Try English version if not already tried
        if needs_trans and not response.meta.get('tried_english', False):
            english_url = self.find_english_url(company_info['domain'])
            if english_url and english_url != response.url:
                yield scrapy.Request(
                    url=english_url,
                    callback=self.parse_company,
                    meta={'company_info': company_info, 'tried_english': True},
                    dont_filter=True
                )
                return
        
        # Translation processing
        translated_soup = soup
        if needs_trans and detected_lang:
            try:
                translated_text = self.translate_key_content(soup, detected_lang)
                if translated_text:
                    translated_soup = BeautifulSoup(translated_text, 'html.parser')
                    result['translation_used'] = True
            except Exception:
                result['translation_used'] = False
        
        # Data extraction
        employee_info = self.extract_employee_count(translated_soup, response.text)
        result.update(employee_info)
        
        location_info = self.extract_location_info(soup, response.text, company_info['domain'])
        result.update(location_info)
        
        industry_info = self.extract_industry_info(soup, response.text, company_info['name'])
        result.update(industry_info)
        
        result['size_category'] = self.determine_size_category(result['employee_count_range'])
        
        # Additional page discovery
        if (not result['employee_count'] or 
            result['industry'] == 'Unknown' or 
            needs_trans):
            
            additional_urls = self.find_relevant_pages(soup, response.url)
            if additional_urls:
                for url in additional_urls[:2]:  # Max 2 additional pages
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_additional_page,
                        meta={'result': result, 'page_type': self.classify_page_type(url)}
                    )
                return
        
        yield result
        
    except Exception as e:
        self.logger.error(f"Error parsing {company_info['name']}: {e}")
        result['error'] = str(e)
        yield result
```

### Size Category Determination

```python
def determine_size_category(self, employee_range):
    """Map employee ranges to business size categories"""
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
```

### Final Result Storage

```python
def closed(self, reason):
    """Save results to CSV when spider closes"""
    self.save_results()

def save_results(self):
    """Save all results to timestamped CSV file"""
    if not self.results:
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
        
        print(f"✅ Results saved to {output_file}")
        
    except Exception as e:
        self.logger.error(f"Error saving results: {e}")
```

---

## Kubernetes Deployment Architecture

### High-Level K8s Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            KUBERNETES CLUSTER                              │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐      │
│  │   Ingress       │    │    ConfigMaps   │    │    Secrets      │      │
│  │   Controller    │    │  - Spider Config│    │  - DB Creds     │      │
│  │                 │    │  - Reference    │    │  - API Keys     │      │
│  │  ┌───────────┐  │    │    Data CSVs    │    │                 │      │
│  │  │Dashboard  │  │    │                 │    │                 │      │
│  │  │   Route   │  │    │                 │    │                 │      │
│  │  └───────────┘  │    │                 │    │                 │      │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘      │
│                                   │                                       │
│  ┌─────────────────────────────────┼─────────────────────────────────┐   │
│  │              SERVICE LAYER      │                                 │   │
│  │                                 ▼                                 │   │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐│   │
│  │  │   Analytics     │    │    Queue        │    │   Database      ││   │
│  │  │   Dashboard     │    │   Service       │    │   Service       ││   │
│  │  │   (Flask)       │    │   (Redis)       │    │ (PostgreSQL)    ││   │
│  │  │                 │    │                 │    │                 ││   │
│  │  │ Port: 5001      │    │ Port: 6379      │    │ Port: 5432      ││   │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────┘│   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                   │                                       │
│  ┌─────────────────────────────────┼─────────────────────────────────┐   │
│  │             WORKER LAYER        │                                 │   │
│  │                                 ▼                                 │   │
│  │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐│   │
│  │  │  Spider Worker  │    │  Spider Worker  │    │  Spider Worker  ││   │
│  │  │     Pod 1       │    │     Pod 2       │    │     Pod N       ││   │
│  │  │                 │    │                 │    │                 ││   │
│  │  │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ ││   │
│  │  │ │CompanyScraper│ │    │ │CompanyScraper│ │    │ │CompanyScraper│ ││   │
│  │  │ │   Process    │ │    │ │   Process    │ │    │ │   Process    │ ││   │
│  │  │ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ ││   │
│  │  └─────────────────┘    └─────────────────┘    └─────────────────┘│   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

### Database Schema (PostgreSQL)

```sql
-- Companies table (source of truth)
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed
    assigned_worker VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Scraped data table
CREATE TABLE company_data (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    url VARCHAR(500),
    status_code INTEGER,
    detected_language VARCHAR(10),
    translated BOOLEAN DEFAULT FALSE,
    employee_count INTEGER,
    employee_count_range VARCHAR(50),
    region VARCHAR(50),
    industry VARCHAR(100),
    size_category VARCHAR(50),
    reasoning TEXT,
    scraped_at TIMESTAMP DEFAULT NOW(),
    worker_id VARCHAR(100)
);

-- Company ratings table  
CREATE TABLE company_ratings (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    score DECIMAL(5,2),
    grade CHAR(1),
    priority VARCHAR(20),
    potential_value DECIMAL(10,2),
    factors JSONB,
    recommendation TEXT,
    rated_at TIMESTAMP DEFAULT NOW()
);

-- Worker status tracking
CREATE TABLE worker_status (
    worker_id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(20), -- active, idle, error
    current_company_id INTEGER,
    last_heartbeat TIMESTAMP DEFAULT NOW(),
    companies_processed INTEGER DEFAULT 0
);
```

### Spider Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spider-workers
spec:
  replicas: 5
  selector:
    matchLabels:
      app: spider-worker
  template:
    metadata:
      labels:
        app: spider-worker
    spec:
      containers:
      - name: spider-worker
        image: company-scraper:latest
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-secret
              key: connection-string
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi" 
            cpu: "500m"
        volumeMounts:
        - name: config-volume
          mountPath: /app/data
          readOnly: true
      volumes:
      - name: config-volume
        configMap:
          name: scraper-config
```

### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: spider-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: spider-workers
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: External
    external:
      metric:
        name: redis_queue_length
      target:
        type: Value
        value: "10"
```

### Modified Spider for Kubernetes

```python
class K8sCompanyScraper(CompanyScraper):
    def __init__(self, worker_id, db_connection, redis_connection):
        super().__init__()
        self.worker_id = worker_id
        self.db = db_connection
        self.redis = redis_connection
        
    def get_next_company(self):
        """Get next company from queue with database locking"""
        company_data = self.redis.lpop('company_queue')
        if company_data:
            company = json.loads(company_data)
            # Update database to mark as processing
            self.db.execute("""
                UPDATE companies 
                SET status = 'processing', 
                    assigned_worker = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (self.worker_id, company['id']))
            return company
        return None
    
    def save_result(self, company_id, result):
        """Save scraping result to database"""
        with self.db.transaction():
            # Insert scraped data
            self.db.execute("""
                INSERT INTO company_data 
                (company_id, url, status_code, detected_language, 
                 translated, employee_count, employee_count_range,
                 region, industry, size_category, reasoning, worker_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (company_id, result['url'], result['status'], 
                  result.get('detected_language'), result.get('translated'),
                  result.get('employee_count'), result.get('employee_count_range'),
                  result.get('region'), result.get('industry'), 
                  result.get('size_category'), result.get('reasoning'), 
                  self.worker_id))
            
            # Update company status
            self.db.execute("""
                UPDATE companies 
                SET status = 'completed', updated_at = NOW()
                WHERE id = %s
            """, (company_id,))
```

### Performance Projections

```
Scaling Parameters:
├── CPU: 250m request, 500m limit per pod
├── Memory: 512Mi request, 1Gi limit per pod
├── Concurrent requests: 2-3 per worker
├── Download delay: 1.5s (respects rate limits)
└── Timeout: 20s per request

Scaling Triggers:
├── Queue depth > 10 companies → Scale up
├── CPU utilization > 70% → Scale up
├── Worker idle time > 5min → Scale down
└── Error rate > 20% → Circuit breaker

Resource Estimates:
├── 1000 companies ≈ 2-3 hours with 5 workers
├── 10000 companies ≈ 1 day with 10-15 workers
├── Database: ~1MB per 1000 companies
└── Translation API: ~$0.50 per 1000 non-English companies
```

---

## Data Flow Journey

### Step-by-Step Data Journey

```
Input Files:
├── data/companies.csv              (source companies)
├── data/industry.csv               (reference data)
├── data/regions.csv                (reference data)  
├── data/headcount.csv              (reference data)
├── data/size.csv                   (reference data)
└── data/deals/deals.csv            (historical deals)
                    │
                    ▼
            [Spider Processing]
                    │
                    ▼
Output Files:
├── output/company_data_TIMESTAMP.csv       (raw scraping results)
├── output/final_company_data_TIMESTAMP.csv (merged batch results)
├── output/company_ratings_TIMESTAMP.csv    (rated prospects - simple)
├── output/company_ratings_TIMESTAMP.json   (rated prospects - detailed)
└── output/scraper.log                      (processing logs)
```

### Detailed Data Processing Steps

1. **Input Stage - Data Loading**
   - `data/companies.csv` read with pandas (semicolon-separated)
   - Format: `Company;Domain;;;`
   - Stored as `{'name': 'CHIMEC', 'domain': 'chimec.de'}`

2. **Reference Data Loading**
   - Industry keywords, regions, headcount categories loaded
   - Used for validation and categorization

3. **Spider Request Generation**
   - Each company becomes a Scrapy Request
   - English URL discovery attempted first
   - Metadata attached for processing

4. **Web Scraping & Processing**
   - HTTP GET request to company website
   - BeautifulSoup HTML parsing
   - Language detection and translation
   - Content extraction with regex patterns

5. **Data Extraction Process**
   - Employee count: 50+ patterns, false positive filtering
   - Industry: Weighted keyword scoring across 8 categories
   - Region: Domain analysis (.de→DACH) + content search
   - Size: Employee count mapped to business categories

6. **Result Compilation**
   - All extracted data merged into result dictionary
   - Reasoning explanations generated
   - Timestamps and metadata added

7. **Storage**
   - Results accumulated in memory during scraping
   - Written to timestamped CSV file on completion
   - Format: `output/company_data_20250801_205103.csv`

---

## Spider Execution Flow

### Complete Execution Path

```
1. INITIALIZATION
   ├── Load reference data (industries, regions, etc.)
   ├── Load company list from CSV
   ├── Setup industry keywords
   └── Initialize Scrapy settings

2. REQUEST GENERATION
   ├── For each company:
   │   ├── Try to find English URL
   │   ├── Create Scrapy Request
   │   └── Set callback to parse_company()
   └── Queue all requests

3. WEB SCRAPING
   ├── HTTP GET to company website
   ├── Parse HTML with BeautifulSoup
   ├── Detect page language
   ├── Try English version if non-English
   └── Translate content if needed

4. DATA EXTRACTION
   ├── Employee Count:
   │   ├── Normalize numbers (3,000 → 3000)
   │   ├── Apply 50+ regex patterns
   │   ├── Filter false positives
   │   └── Select highest valid count
   ├── Region:
   │   ├── Check domain extension (.de → DACH)
   │   ├── Search content for countries
   │   └── Default to EU
   └── Industry:
       ├── Score keywords across content sources
       ├── Weight by source reliability
       └── Select highest scoring industry

5. ADDITIONAL PAGES
   ├── Find relevant links (about, team, contact)
   ├── Translate navigation text if needed
   ├── Visit up to 2 additional pages
   └── Update results with new information

6. RESULT COMPILATION
   ├── Merge all extracted data
   ├── Determine size category
   ├── Create reasoning explanations
   └── Add timestamps and metadata

7. STORAGE
   ├── Accumulate results in memory
   ├── Write to timestamped CSV file
   └── Log completion statistics
```

---

## Summary

This spider system represents a sophisticated approach to web scraping with:

### Technical Sophistication
- **50+ regex patterns** for multilingual employee count detection
- **Advanced translation pipeline** with timeout protection and fallback strategies
- **Intelligent page discovery** using link analysis and navigation translation
- **Weighted scoring algorithms** for industry classification
- **Domain-priority region detection** with text-based fallbacks

### Scalability Features
- **Kubernetes-ready architecture** with database integration
- **Horizontal auto-scaling** based on queue depth and CPU usage
- **Fault-tolerant design** with retry mechanisms and circuit breakers
- **Resource optimization** with spot instances and translation caching

### Data Quality Assurance
- **False positive filtering** to remove years, customer counts, etc.
- **Context analysis** to validate extracted information
- **Multiple source validation** with weighted confidence scoring
- **Comprehensive logging** with reasoning explanations

The system successfully balances performance, accuracy, and scalability while handling the complexities of multilingual web scraping at scale.