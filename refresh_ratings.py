#!/usr/bin/env python3
"""
Refresh company ratings after updating deals data
"""

import os
import glob
import sys

def refresh_ratings():
    """Delete existing ratings to force regeneration"""
    
    # Find and delete existing rating files
    rating_files = glob.glob('output/company_ratings_*.json') + glob.glob('output/company_ratings_*.csv')
    
    if rating_files:
        print(f"Found {len(rating_files)} existing rating files:")
        for file in rating_files:
            print(f"  - {file}")
        
        response = input("\nDelete these files to force rating regeneration? [y/N]: ")
        
        if response.lower() == 'y':
            for file in rating_files:
                os.remove(file)
                print(f"Deleted: {file}")
            
            print("\n✅ Rating files deleted!")
            print("\nNext steps:")
            print("1. Make sure deals.csv has been updated")
            print("2. Restart the analytics app:")
            print("   - Kill existing: lsof -ti:5002 | xargs kill")
            print("   - Start new: python analytics_app.py")
            print("\nThe analytics app will automatically generate new ratings on next access.")
        else:
            print("Cancelled. No files deleted.")
    else:
        print("No existing rating files found.")
        print("\nIf you've updated deals.csv, just restart the analytics app:")
        print("   - Kill existing: lsof -ti:5002 | xargs kill")
        print("   - Start new: python analytics_app.py")

if __name__ == "__main__":
    refresh_ratings()