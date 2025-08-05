from flask import Flask, render_template, request, jsonify, make_response
import pandas as pd
import os
import json
import io
from datetime import datetime
from company_rater import CompanyRater

app = Flask(__name__)

def load_deals_data():
    df = pd.read_csv('data/deals/deals.csv', delimiter=';')
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    return df

def get_unique_values(df):
    return {
        'industries': sorted(df['Industry'].unique()),
        'regions': sorted(df['Region'].unique()),
        'headcount_ranges': sorted(df['Company Headcount Range'].unique(), 
                                  key=lambda x: int(x.split('-')[0]) if '-' in x else float('inf')),
        'stages': sorted(df['Stage'].unique())
    }

@app.route('/')
def dashboard():
    df = load_deals_data()
    
    # Get filter parameters
    industry = request.args.get('industry', '')
    region = request.args.get('region', '')
    headcount = request.args.get('headcount', '')
    stage = request.args.get('stage', '')
    sort_by = request.args.get('sort_by', 'Amount')
    sort_order = request.args.get('sort_order', 'desc')
    
    # Apply filters
    if industry:
        df = df[df['Industry'] == industry]
    if region:
        df = df[df['Region'] == region]
    if headcount:
        df = df[df['Company Headcount Range'] == headcount]
    if stage:
        df = df[df['Stage'] == stage]
    
    # Sort data
    ascending = sort_order == 'asc'
    if sort_by in df.columns:
        df = df.sort_values(by=sort_by, ascending=ascending)
    
    # Calculate statistics
    stats = {
        'total_deals': len(df),
        'total_amount': df['Amount'].sum(),
        'won_deals': len(df[df['Stage'] == 'Closed Won']),
        'lost_deals': len(df[df['Stage'] == 'Closed Lost']),
        'win_rate': round(len(df[df['Stage'] == 'Closed Won']) / len(df) * 100, 1) if len(df) > 0 else 0,
        'avg_deal_size': round(df['Amount'].mean(), 2) if len(df) > 0 else 0
    }
    
    # Get unique values for filters
    unique_values = get_unique_values(load_deals_data())
    
    # Convert dataframe to dict for template
    deals = df.to_dict('records')
    
    return render_template('dashboard.html', 
                         deals=deals, 
                         stats=stats,
                         unique_values=unique_values,
                         filters={
                             'industry': industry,
                             'region': region,
                             'headcount': headcount,
                             'stage': stage,
                             'sort_by': sort_by,
                             'sort_order': sort_order
                         })

@app.route('/prospects')
def prospects():
    # Get the latest scraped data and ratings files
    scraped_files = [f for f in os.listdir('output') if f.startswith('final_company_data_') and f.endswith('.csv')]
    rating_files = [f for f in os.listdir('output') if f.startswith('company_ratings_') and f.endswith('.json')]
    
    if not scraped_files:
        return "No scraped company data found. Please run the scraper first.", 404
    
    latest_scraped = sorted(scraped_files)[-1]
    
    # Check if we need to generate new ratings
    need_new_ratings = False
    
    if not rating_files:
        need_new_ratings = True
        print("No ratings file found, generating new ratings...")
    else:
        latest_rating_file = sorted(rating_files)[-1]
        
        # Extract timestamps from filenames to compare
        scraped_timestamp = latest_scraped.split('_')[2] + '_' + latest_scraped.split('_')[3].replace('.csv', '')
        rating_timestamp = latest_rating_file.split('_')[2] + '_' + latest_rating_file.split('_')[3].replace('.json', '')
        
        if scraped_timestamp > rating_timestamp:
            need_new_ratings = True
            print(f"Scraped data ({scraped_timestamp}) is newer than ratings ({rating_timestamp}), generating new ratings...")
    
    if need_new_ratings:
        # Generate new ratings
        rater = CompanyRater()
        ratings = rater.rate_companies_batch(f'output/{latest_scraped}')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        rater.save_ratings(ratings, f'output/company_ratings_{timestamp}')
        latest_rating_file = f'company_ratings_{timestamp}.json'
    else:
        latest_rating_file = sorted(rating_files)[-1]
    
    # Load ratings
    with open(f'output/{latest_rating_file}', 'r') as f:
        ratings = json.load(f)
    
    # Get filter parameters
    grade = request.args.get('grade', '')
    priority = request.args.get('priority', '')
    industry = request.args.get('industry', '')
    region = request.args.get('region', '')
    min_score = request.args.get('min_score', '')
    
    # Filter ratings
    filtered_ratings = ratings
    if grade:
        filtered_ratings = [r for r in filtered_ratings if r['grade'] == grade]
    if priority:
        filtered_ratings = [r for r in filtered_ratings if r['priority'] == priority]
    if industry:
        filtered_ratings = [r for r in filtered_ratings if r.get('factors', {}).get('industry', {}).get('value', '') == industry]
    if region:
        filtered_ratings = [r for r in filtered_ratings if r.get('factors', {}).get('region', {}).get('value', '') == region]
    if min_score:
        try:
            min_score_val = float(min_score)
            filtered_ratings = [r for r in filtered_ratings if r['score'] >= min_score_val]
        except:
            pass
    
    # Get unique values for filters
    unique_values = {
        'grades': sorted(list(set(r['grade'] for r in ratings))),
        'priorities': sorted(list(set(r['priority'] for r in ratings)), 
                           key=lambda x: ['High', 'Medium', 'Low', 'Very Low'].index(x)),
        'industries': sorted(list(set(r.get('factors', {}).get('industry', {}).get('value', '') for r in ratings if r.get('factors', {}).get('industry', {}).get('value', '')))),
        'regions': sorted(list(set(r.get('factors', {}).get('region', {}).get('value', '') for r in ratings if r.get('factors', {}).get('region', {}).get('value', ''))))
    }
    
    # Calculate statistics
    stats = {
        'total_prospects': len(filtered_ratings),
        'grade_a': len([r for r in filtered_ratings if r['grade'] == 'A']),
        'grade_b': len([r for r in filtered_ratings if r['grade'] == 'B']),
        'grade_c': len([r for r in filtered_ratings if r['grade'] == 'C']),
        'grade_d': len([r for r in filtered_ratings if r['grade'] == 'D']),
        'avg_score': round(sum(r['score'] for r in filtered_ratings) / len(filtered_ratings), 1) if filtered_ratings else 0,
        'total_potential_value': sum(r['potential_value'] for r in filtered_ratings)
    }
    
    return render_template('prospects.html',
                         ratings=filtered_ratings,
                         stats=stats,
                         unique_values=unique_values,
                         filters={
                             'grade': grade,
                             'priority': priority,
                             'industry': industry,
                             'region': region,
                             'min_score': min_score
                         })

@app.route('/rate_companies', methods=['POST'])
def rate_companies():
    """Endpoint to trigger rating of newly scraped companies"""
    rater = CompanyRater()
    
    # Find the latest scraped data
    scraped_files = [f for f in os.listdir('output') if f.startswith('final_company_data_') and f.endswith('.csv')]
    if not scraped_files:
        return jsonify({'error': 'No scraped company data found'}), 404
    
    latest_scraped = sorted(scraped_files)[-1]
    
    # Rate companies
    ratings = rater.rate_companies_batch(f'output/{latest_scraped}')
    
    # Save ratings
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file, csv_file = rater.save_ratings(ratings, f'output/company_ratings_{timestamp}')
    
    return jsonify({
        'success': True,
        'rated_count': len(ratings),
        'json_file': json_file,
        'csv_file': csv_file
    })

@app.route('/export_prospects')
def export_prospects():
    """Export filtered prospects to CSV"""
    # Get the latest ratings file
    rating_files = [f for f in os.listdir('output') if f.startswith('company_ratings_') and f.endswith('.json')]
    if not rating_files:
        return "No rated prospects found. Please rate companies first.", 404
    
    latest_rating_file = sorted(rating_files)[-1]
    
    # Load ratings
    with open(f'output/{latest_rating_file}', 'r') as f:
        ratings = json.load(f)
    
    # Get filter parameters (same as prospects route)
    grade = request.args.get('grade', '')
    priority = request.args.get('priority', '')
    industry = request.args.get('industry', '')
    region = request.args.get('region', '')
    min_score = request.args.get('min_score', '')
    
    # Filter ratings
    filtered_ratings = ratings
    if grade:
        filtered_ratings = [r for r in filtered_ratings if r['grade'] == grade]
    if priority:
        filtered_ratings = [r for r in filtered_ratings if r['priority'] == priority]
    if industry:
        filtered_ratings = [r for r in filtered_ratings if r.get('factors', {}).get('industry', {}).get('value', '') == industry]
    if region:
        filtered_ratings = [r for r in filtered_ratings if r.get('factors', {}).get('region', {}).get('value', '') == region]
    if min_score:
        try:
            min_score_val = float(min_score)
            filtered_ratings = [r for r in filtered_ratings if r['score'] >= min_score_val]
        except:
            pass
    
    # Convert to CSV format
    csv_data = []
    for rating in filtered_ratings:
        csv_data.append({
            'Company Name': rating['company_name'],
            'Domain': rating['domain'],
            'Score': rating['score'],
            'Grade': rating['grade'],
            'Priority': rating['priority'],
            'Industry': rating.get('factors', {}).get('industry', {}).get('value', ''),
            'Region': rating.get('factors', {}).get('region', {}).get('value', ''),
            'Size': rating.get('factors', {}).get('size', {}).get('value', ''),
            'Potential Value': rating['potential_value'],
            'Industry Win Rate %': round(rating.get('factors', {}).get('industry', {}).get('win_rate', 0), 1),
            'Region Win Rate %': round(rating.get('factors', {}).get('region', {}).get('win_rate', 0), 1),
            'Size Win Rate %': round(rating.get('factors', {}).get('size', {}).get('win_rate', 0), 1),
            'Data Quality Score': rating.get('factors', {}).get('data_quality', {}).get('score', 0),
            'Recommendation': rating['recommendation'],
            'Rated At': rating['rated_at']
        })
    
    # Create CSV
    df = pd.DataFrame(csv_data)
    
    # Create CSV response
    output = io.StringIO()
    df.to_csv(output, index=False)
    csv_content = output.getvalue()
    output.close()
    
    # Create response with proper headers
    response = make_response(csv_content)
    response.headers['Content-Type'] = 'text/csv'
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"prospects_export_{timestamp}.csv"
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    
    return response

if __name__ == '__main__':
    app.run(debug=True, port=5002)
