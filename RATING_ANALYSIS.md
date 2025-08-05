# Company Rating Analysis - Why No A/B Grades?

## Summary
Out of 1,191 rated companies, there are:
- **Grade A (80-100)**: 0 companies
- **Grade B (60-79)**: 0 companies  
- **Grade C (40-59)**: 442 companies (37.1%)
- **Grade D (0-39)**: 749 companies (62.9%)

The highest score achieved is only **49.2 out of 100**.

## Root Cause: Low Historical Win Rates

The rating algorithm scores companies based on historical deal performance from `data/deals/deals.csv`. The problem is that the historical data shows very poor performance:

### 1. Industry Performance (35% of score)
Best performing industries have low win rates:
- Manufacturing: 38.6% win rate → 13.5/35 points
- Real Estate: 26.3% win rate → 9.2/35 points
- Software: 24.1% win rate → 8.4/35 points

### 2. Regional Performance (25% of score)
All regions show poor performance:
- EU: 30.4% win rate → 7.6/25 points
- UKI: 25.0% win rate → 6.2/25 points
- DACH: 23.0% win rate → 5.7/25 points

### 3. Company Size Performance (25% of score)
Surprisingly, larger companies perform WORSE:
- Very Small Business: 24.1% win rate
- Small Business: 23.9% win rate
- Mid-Market: 19.0% win rate
- **Enterprise: 6.9% win rate** (worst!)

Additionally, 79.2% of scraped companies have "Unknown" size, defaulting to 10/25 points.

### 4. Overall Statistics
- **Overall historical win rate: 22.0%**
- This means 78% of past deals were lost

## Maximum Theoretical Score

Even a perfect company with:
- Best industry (Manufacturing)
- Best region (EU)
- Best size (Very Small Business)
- Perfect data quality

Would only score **~56 points** (Grade C).

To get Grade B (60+ points), you'd need the best size category with employee data, which most companies lack.

## Recommendations

1. **Adjust the grading scale** to match your historical performance:
   - Grade A: 45-100 (top 10%)
   - Grade B: 40-45 (next 20%)
   - Grade C: 35-40 (next 30%)
   - Grade D: 0-35 (bottom 40%)

2. **Improve data collection** to get employee counts for more companies

3. **Review historical deals data** - the 22% win rate might indicate:
   - Very selective sales process
   - Data quality issues
   - Need to reassess ideal customer profile

4. **Consider alternative scoring** that doesn't rely solely on historical win rates