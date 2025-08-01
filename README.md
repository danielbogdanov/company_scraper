# Company Data Scraper

A comprehensive web scraper built with Python, BeautifulSoup, Scrapy, and translation services to extract company information including employee count, region, and industry classification from company websites.

## Features

- 🌐 **Multi-language Support**: Automatically detects non-English content and translates it using Google Translate
- 🔍 **Smart Page Discovery**: Finds about-us and company pages using multilingual keyword detection
- 📊 **Employee Count Extraction**: Uses comprehensive regex patterns to detect employee counts from translated content
- 🗺️ **Region Detection**: Prioritizes domain-based detection with fallback to content analysis
- 🏭 **Industry Classification**: Keyword-based industry detection with weighted scoring
- 📝 **Comprehensive Logging**: Detailed reasoning for all categorizations
- 📈 **Scalable Architecture**: Built with Scrapy for handling large-scale scraping operations

## Requirements

- Python 3.7+
- All dependencies listed in `requirements.txt`

## Installation

```bash
# Install dependencies
pip install -r requirements.txt
```

## Data Files

The scraper requires the following CSV data files in the `data/` directory:

- `companies.csv` - Companies to scrape (format: Company;Domain)
- `industry.csv` - Industry categories
- `regions.csv` - Region categories  
- `headcount.csv` - Employee count ranges
- `size.csv` - Company size categories

## Usage

### Basic Usage

```bash
python run_scraper.py
```

### Advanced Options

```bash
# Process only first 10 companies with debug logging
python run_scraper.py --max-companies 10 --log-level DEBUG

# Use custom companies file
python run_scraper.py --companies-file data/my_companies.csv

# Custom output directory
python run_scraper.py --output-dir results/
```

### Command Line Options

- `--companies-file`: Path to companies CSV file (default: data/companies.csv)
- `--max-companies`: Maximum number of companies to process (default: all)
- `--output-dir`: Output directory for results (default: output/)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR) (default: INFO)

## How It Works

### 1. Language Detection & Translation
- Automatically detects page language using `langdetect`
- Attempts to find English versions of websites first
- Falls back to translating key content sections if no English version found
- Only processes translated content to ensure accurate regex matching

### 2. Employee Count Detection
The scraper uses comprehensive regex patterns optimized for translated content:
- Direct patterns: "X employees", "X people", "X staff"
- Contextual patterns: "team of X", "workforce of X"
- Translation-aware patterns: handles common translation quirks
- Range patterns: extracts lower bound from ranges like "50-100 employees"

### 3. Region Detection
- **Primary**: Domain extension analysis (.nl → BeNeLux, .de → DACH, etc.)
- **Secondary**: Country name detection in content
- **Fallback**: Defaults to 'EU' if no specific region found

### 4. Industry Classification
- Keyword-based matching with weighted scoring
- Prioritizes translated content for better accuracy
- Uses company name, page title, meta description, and content
- Requires minimum confidence score to avoid false positives

### 5. Size Categorization
Based on employee count ranges:
- Very Small Business: 1-50 employees
- Small Business: 51-200 employees  
- Mid-Market: 201-1000 employees
- Enterprise: 1000+ employees

## Output

The scraper generates:

### CSV Results
Timestamped CSV files in the `output/` directory with columns:
- `company_name`: Company name
- `domain`: Company domain
- `url`: Scraped URL
- `status`: HTTP status or 'error'
- `detected_language`: Detected page language
- `translated`: Whether translation was used
- `employee_count`: Exact employee count (if found)
- `employee_count_range`: Categorized range
- `region`: Detected region
- `industry`: Classified industry
- `size_category`: Company size category
- `reasoning`: Detailed reasoning for categorizations
- `scraped_at`: Timestamp

### Log Files
Detailed logs with:
- Language detection results
- Translation attempts and results
- Pattern matching details
- Categorization reasoning
- Error handling and fallbacks

## Architecture

### Core Components

- **CompanyScraper**: Main Scrapy spider class
- **Translation Service**: Google Translate integration via deep-translator
- **Pattern Matching**: Comprehensive regex patterns for employee detection
- **Region Mapper**: Domain and content-based region detection
- **Industry Classifier**: Keyword-based industry classification
- **Results Handler**: CSV output and logging management

### Key Features

- **Scalability**: Built on Scrapy for concurrent processing
- **Error Handling**: Comprehensive error handling with graceful degradation
- **Caching**: HTTP caching to avoid re-downloading pages
- **Rate Limiting**: Respectful crawling with delays and throttling
- **Translation Optimization**: Extracts and translates only key content sections

## Example Output

```csv
company_name,domain,employee_count,employee_count_range,region,industry,size_category,reasoning
"IVC Evidensia Nederland",evidensia.nl,2500,"501-1000",BeNeLux,"Healthcare, Pharmaceuticals, & Biotech",Mid-Market,"Region: Domain extension .nl indicates BeNeLux; Industry: Keywords matched: veterinary, animal hospital; Score: 12.0"
"KiK Polska",kik.pl,150,"101-200",EU,"Retail (incl. Restaurants)",Small Business,"Region: Domain extension .pl indicates EU; Industry: Keywords matched: fashion, clothing, retail; Score: 8.5"
```

## Troubleshooting

### Common Issues

1. **Translation Failures**: The scraper gracefully handles translation failures by processing original content
2. **Rate Limiting**: Built-in delays and throttling prevent being blocked
3. **Missing Data**: Comprehensive fallbacks ensure all companies get processed
4. **Language Detection**: Handles short content and ambiguous languages

### Debugging

Use `--log-level DEBUG` for detailed information:
```bash
python run_scraper.py --log-level DEBUG --max-companies 5
```

### Performance Tuning

The scraper includes several performance optimizations:
- HTTP caching for repeated requests
- Concurrent processing with rate limiting
- Translation timeout protection
- Key content extraction (not full page translation)

## Extending the Scraper

### Adding New Industries
Edit `data/industry.csv` and update the industry keywords in `CompanyScraper.setup_industry_keywords()`

### Adding New Regions
Edit `data/regions.csv` and update the region mapping in `CompanyScraper.extract_region()`

### Custom Translation Service
Replace the `translate_text()` method to use different translation APIs (DeepL, Azure, etc.)

## License

This project is for educational and research purposes. Please respect robots.txt and website terms of service when scraping.