# Company Scoring System Documentation

## Overview

The company scoring system evaluates prospects on a **100-point scale** based on historical deal performance data. Companies are assigned grades A-D based on their total score, with recommendations for sales prioritization.

## Scoring Components

### 1. Industry Score (35 points) - 35% of total
**The most heavily weighted factor**

- Based on historical win rate for the company's industry
- Formula: `(industry_win_rate / 100) × 35`
- Data source: Historical deals grouped by industry

**Example:**
- Manufacturing has 38.6% historical win rate
- Score: 38.6% × 35 = 13.5 points

### 2. Region Score (25 points) - 25% of total
**Geographic success patterns**

- Based on historical win rate for the company's region
- Formula: `(region_win_rate / 100) × 25`
- Regions: BeNeLux, DACH, ES, EU, FR, UKI

**Example:**
- EU region has 30.4% historical win rate
- Score: 30.4% × 25 = 7.6 points

### 3. Company Size Score (25 points) - 25% of total
**Company size category performance**

- Based on historical win rate for the company's size category
- Formula: `(size_win_rate / 100) × 25`
- Size categories:
  - Very Small Business (1-50 employees)
  - Small Business (51-200 employees)
  - Mid-Market (201-1000 employees)
  - Enterprise (1000+ employees)
- **Special case**: If size is "Unknown" → default 10 points

**Example:**
- Very Small Business has 24.1% historical win rate
- Score: 24.1% × 25 = 6.0 points

### 4. Data Quality Score (15 points) - 15% of total
**Rewards complete data collection**

Not based on win rates, but on data completeness:
- Successful web scrape (HTTP 200 status): **+5 points**
- Employee count/range available: **+5 points**
- Industry identified (not "Unknown"): **+5 points**

**Example:**
- All three criteria met = 15/15 points

## Grade Assignment

Total scores are converted to letter grades:

| Grade | Score Range | Priority | Description |
|-------|-------------|----------|-------------|
| **A** | 80-100 | High | Top prospects with strongest success indicators |
| **B** | 60-79 | Medium | Good prospects with above-average potential |
| **C** | 40-59 | Low | Lower priority, bulk campaign candidates |
| **D** | 0-39 | Very Low | Minimal success probability based on history |

## Calculation Example

**Company Profile:**
- Industry: Software & Internet (24.1% win rate)
- Region: DACH (23.0% win rate)
- Size: Unknown
- Data: All fields successfully collected

**Score Calculation:**
```
Industry Score:     24.1% × 35 = 8.4 points
Region Score:       23.0% × 25 = 5.8 points
Size Score:         Unknown    = 10.0 points (default)
Data Quality:       Perfect    = 15.0 points
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TOTAL SCORE:                    39.2 points

GRADE: D (Very Low Priority)
```

## Win Rate Calculation

Win rates are calculated from `data/deals/deals.csv`:

```
Win Rate = (Closed Won Deals / Total Deals) × 100%
```

**Example - Manufacturing Industry:**
- Total deals: 70
- Closed Won: 27
- Closed Lost: 43
- Win Rate: 27/70 = 38.6%

## Current Performance Metrics

Based on the historical deals data:

### Industry Win Rates
1. Manufacturing: 38.6% (best)
2. Real Estate: 26.3%
3. Software: 24.1%
4. Healthcare: 20.5%
5. Business Services: 17.7%
6. Financial Services: 12.8% (worst)

### Regional Win Rates
1. EU: 30.4% (best)
2. UKI: 25.0%
3. DACH: 23.0%
4. ES: 21.8%
5. BeNeLux: 16.2%
6. FR: 15.1% (worst)

### Size Category Win Rates
1. Very Small Business: 24.1% (best)
2. Small Business: 23.9%
3. Mid-Market: 19.0%
4. Enterprise: 6.9% (worst)

### Overall Statistics
- **Overall win rate**: 22.0%
- **Overall loss rate**: 78.0%

## Recommendation Templates

Based on final grades, companies receive these recommendations:

### Grade A (80-100 points)
> "High priority prospect. Strong historical success indicators across multiple factors. Immediate outreach recommended."

### Grade B (60-79 points)
> "Good prospect with above-average success potential. Include in targeted campaigns."

### Grade C (40-59 points)
> "Lower priority. Consider for bulk campaigns or when higher-rated prospects are exhausted."

### Grade D (0-39 points)
> "Very low priority. Historical patterns suggest low success probability."

## Potential Value Calculation

Each company also receives a potential deal value estimate based on:
- Industry average deal size
- Company size multiplier
- Regional factors

Formula: `base_industry_value × size_multiplier × region_multiplier`

## System Limitations

1. **Historical Bias**: Scoring relies entirely on past performance
2. **Data Completeness**: 79% of companies have "Unknown" size, limiting accurate scoring
3. **Low Win Rates**: With 22% overall win rate, achieving high scores is mathematically difficult
4. **Static Patterns**: Doesn't account for market changes or company-specific factors

## Modifying the System

To adjust scoring after updating deals data:

1. Edit `data/deals/deals.csv`
2. Delete existing ratings: `rm output/company_ratings_*.json`
3. Restart analytics app - it will regenerate ratings automatically

The system will recalculate all win rates and re-score all companies based on the new data.