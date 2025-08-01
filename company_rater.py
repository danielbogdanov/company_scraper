import pandas as pd
import numpy as np
from datetime import datetime
import json

class CompanyRater:
    def __init__(self):
        # Load historical deal patterns
        self.deals_df = pd.read_csv('data/deals/deals.csv', delimiter=';')
        self.patterns = self._analyze_patterns()
        
    def _analyze_patterns(self):
        """Analyze historical deals to extract success patterns"""
        patterns = {}
        
        # Win rates by industry
        industry_group = self.deals_df.groupby('Industry')
        patterns['industry_win_rate'] = (industry_group['Stage'].apply(lambda x: (x == 'Closed Won').sum() / len(x))).to_dict()
        patterns['industry_avg_amount'] = industry_group['Amount'].mean().to_dict()
        patterns['industry_deal_count'] = industry_group.size().to_dict()
        
        # Win rates by region
        region_group = self.deals_df.groupby('Region')
        patterns['region_win_rate'] = (region_group['Stage'].apply(lambda x: (x == 'Closed Won').sum() / len(x))).to_dict()
        patterns['region_avg_amount'] = region_group['Amount'].mean().to_dict()
        
        # Win rates by size
        size_group = self.deals_df.groupby('Company Headcount Size')
        patterns['size_win_rate'] = (size_group['Stage'].apply(lambda x: (x == 'Closed Won').sum() / len(x))).to_dict()
        patterns['size_avg_amount'] = size_group['Amount'].mean().to_dict()
        
        # Overall stats
        patterns['overall_win_rate'] = (self.deals_df['Stage'] == 'Closed Won').sum() / len(self.deals_df)
        patterns['overall_avg_amount'] = self.deals_df['Amount'].mean()
        
        return patterns
    
    def _map_headcount_to_size(self, headcount_range):
        """Map headcount range to size category"""
        if pd.isna(headcount_range) or headcount_range == '':
            return 'Unknown'
        
        if headcount_range in ['1-9', '10-20', '10-50']:
            return 'Very Small Business'
        elif headcount_range in ['51-100']:
            return 'Small Business'
        elif headcount_range in ['101-200']:
            return 'Small Business'
        elif headcount_range in ['201-500']:
            return 'Mid-Market'
        elif headcount_range in ['501-1000']:
            return 'Mid-Market'
        elif headcount_range in ['1001-5000']:
            return 'Mid-Market'
        elif headcount_range in ['over 5000']:
            return 'Enterprise'
        else:
            return 'Unknown'
    
    def rate_company(self, company_data):
        """Rate a single company based on historical patterns"""
        score = 0
        max_score = 0
        factors = {}
        
        # 1. Industry Score (0-30 points)
        industry = company_data.get('industry', 'Unknown')
        if industry in self.patterns['industry_win_rate']:
            industry_score = self.patterns['industry_win_rate'][industry] * 30
            industry_deal_count = self.patterns['industry_deal_count'].get(industry, 0)
            # Bonus for industries with more deals (proven market)
            if industry_deal_count > 3:
                industry_score += 5
            factors['industry'] = {
                'value': industry,
                'score': round(industry_score, 1),
                'win_rate': round(self.patterns['industry_win_rate'][industry] * 100, 1),
                'deal_count': industry_deal_count
            }
        else:
            industry_score = self.patterns['overall_win_rate'] * 20  # Default to overall rate with penalty
            factors['industry'] = {
                'value': industry,
                'score': round(industry_score, 1),
                'win_rate': round(self.patterns['overall_win_rate'] * 100, 1),
                'note': 'New industry - using overall average with penalty'
            }
        score += industry_score
        max_score += 35
        
        # 2. Region Score (0-25 points)
        region = company_data.get('region', 'EU')
        if region in self.patterns['region_win_rate']:
            region_score = self.patterns['region_win_rate'][region] * 25
            factors['region'] = {
                'value': region,
                'score': round(region_score, 1),
                'win_rate': round(self.patterns['region_win_rate'][region] * 100, 1)
            }
        else:
            region_score = self.patterns['overall_win_rate'] * 15
            factors['region'] = {
                'value': region,
                'score': round(region_score, 1),
                'note': 'Unknown region - using overall average with penalty'
            }
        score += region_score
        max_score += 25
        
        # 3. Company Size Score (0-25 points)
        size_category = company_data.get('size_category', 'Unknown')
        if size_category == 'Unknown' and company_data.get('employee_count_range'):
            size_category = self._map_headcount_to_size(company_data['employee_count_range'])
        
        if size_category in self.patterns['size_win_rate']:
            size_score = self.patterns['size_win_rate'][size_category] * 25
            factors['size'] = {
                'value': size_category,
                'score': round(size_score, 1),
                'win_rate': round(self.patterns['size_win_rate'][size_category] * 100, 1)
            }
        else:
            size_score = 10  # Default middle score for unknown size
            factors['size'] = {
                'value': size_category,
                'score': size_score,
                'note': 'Unknown size - using default score'
            }
        score += size_score
        max_score += 25
        
        # 4. Data Quality Score (0-15 points)
        data_quality_score = 0
        data_quality_factors = []
        
        # Check if we have good data
        if company_data.get('status') == '200':
            data_quality_score += 5
            data_quality_factors.append('Successful scrape')
        
        if company_data.get('employee_count') or company_data.get('employee_count_range'):
            data_quality_score += 5
            data_quality_factors.append('Employee data available')
        
        if industry != 'Unknown':
            data_quality_score += 5
            data_quality_factors.append('Industry identified')
        
        factors['data_quality'] = {
            'score': data_quality_score,
            'factors': data_quality_factors
        }
        score += data_quality_score
        max_score += 15
        
        # 5. Calculate final score and rating
        final_score = (score / max_score) * 100 if max_score > 0 else 0
        
        # Determine rating grade
        if final_score >= 80:
            grade = 'A'
            priority = 'High'
        elif final_score >= 60:
            grade = 'B'
            priority = 'Medium'
        elif final_score >= 40:
            grade = 'C'
            priority = 'Low'
        else:
            grade = 'D'
            priority = 'Very Low'
        
        # Calculate potential deal value
        potential_value = self._calculate_potential_value(company_data)
        
        return {
            'company_name': company_data.get('company_name', 'Unknown'),
            'domain': company_data.get('domain', ''),
            'score': round(final_score, 1),
            'grade': grade,
            'priority': priority,
            'potential_value': round(potential_value, 0),
            'factors': factors,
            'recommendation': self._generate_recommendation(grade, factors),
            'rated_at': datetime.now().isoformat()
        }
    
    def _calculate_potential_value(self, company_data):
        """Estimate potential deal value based on similar companies"""
        # Start with industry average
        industry = company_data.get('industry', 'Unknown')
        if industry in self.patterns['industry_avg_amount']:
            base_value = self.patterns['industry_avg_amount'][industry]
        else:
            base_value = self.patterns['overall_avg_amount']
        
        # Adjust for company size
        size_category = company_data.get('size_category', 'Unknown')
        if size_category == 'Unknown' and company_data.get('employee_count_range'):
            size_category = self._map_headcount_to_size(company_data['employee_count_range'])
        
        if size_category in self.patterns['size_avg_amount']:
            size_multiplier = self.patterns['size_avg_amount'][size_category] / self.patterns['overall_avg_amount']
            base_value *= size_multiplier
        
        return base_value
    
    def _generate_recommendation(self, grade, factors):
        """Generate actionable recommendation based on rating"""
        if grade == 'A':
            return "Immediate outreach recommended. High probability of success based on historical patterns."
        elif grade == 'B':
            return "Good prospect. Include in regular outreach campaigns."
        elif grade == 'C':
            return "Lower priority. Consider for bulk campaigns or when higher-rated prospects are exhausted."
        else:
            return "Very low priority. May not be worth pursuing unless specific circumstances change."
    
    def rate_companies_batch(self, csv_file):
        """Rate all companies in a CSV file"""
        companies_df = pd.read_csv(csv_file)
        ratings = []
        
        for _, company in companies_df.iterrows():
            rating = self.rate_company(company.to_dict())
            ratings.append(rating)
        
        # Sort by score descending
        ratings.sort(key=lambda x: x['score'], reverse=True)
        
        return ratings
    
    def save_ratings(self, ratings, output_file):
        """Save ratings to JSON and CSV files"""
        # Save detailed JSON
        with open(output_file + '.json', 'w') as f:
            json.dump(ratings, f, indent=2)
        
        # Create simplified CSV
        simplified_ratings = []
        for rating in ratings:
            simplified = {
                'company_name': rating['company_name'],
                'domain': rating['domain'],
                'score': rating['score'],
                'grade': rating['grade'],
                'priority': rating['priority'],
                'potential_value': rating['potential_value'],
                'industry': rating['factors'].get('industry', {}).get('value', ''),
                'region': rating['factors'].get('region', {}).get('value', ''),
                'size': rating['factors'].get('size', {}).get('value', ''),
                'recommendation': rating['recommendation']
            }
            simplified_ratings.append(simplified)
        
        pd.DataFrame(simplified_ratings).to_csv(output_file + '.csv', index=False)
        
        return output_file + '.json', output_file + '.csv'

if __name__ == "__main__":
    # Example usage
    rater = CompanyRater()
    
    # Rate all companies in the scraped data
    ratings = rater.rate_companies_batch('output/final_company_data_20250801_205103.csv')
    
    # Save ratings
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file, csv_file = rater.save_ratings(ratings, f'output/company_ratings_{timestamp}')
    
    print(f"\n=== COMPANY RATING COMPLETE ===")
    print(f"Rated {len(ratings)} companies")
    print(f"\nTop 10 Companies by Score:")
    print("-" * 80)
    print(f"{'Company':<30} {'Score':<8} {'Grade':<6} {'Priority':<10} {'Potential':<10}")
    print("-" * 80)
    
    for rating in ratings[:10]:
        print(f"{rating['company_name'][:29]:<30} {rating['score']:<8} {rating['grade']:<6} {rating['priority']:<10} ${rating['potential_value']:<10,.0f}")
    
    print(f"\nRatings saved to:")
    print(f"  - {json_file} (detailed)")
    print(f"  - {csv_file} (simplified)")