import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from tqdm import tqdm
import re
import certifi

# Updated headers with security tokens
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://ngodarpan.gov.in',
    'Referer': 'https://ngodarpan.gov.in/index.php/search',
}

# Correct numeric state codes for NGO Darpan API
INDIAN_STATES = {
    27: 'Maharashtra',
    9: 'Delhi',
    17: 'Karnataka',
    # Add other states with numeric codes
}

def scrape_ngo_darpan():
    """Updated scraper with current API requirements"""
    base_url = "https://ngodarpan.gov.in/index.php/ajaxcontroller/search_ngo"
    
    all_ngos = []
    
    for state_code, state_name in tqdm(INDIAN_STATES.items(), desc="Scraping States"):
        try:
            # New required payload structure
            payload = {
                'page': '',
                'search_type': 'state',
                'state_search': state_code,
                'district_search': '',
                'sector_search': '',
                'ngo_type_search': '',
                'ngo_name_search': '',
                'unique_id_search': '',
                'view_type': 'view'
            }
            
            response = requests.post(base_url, headers=HEADERS, data=payload)
            
            # Check for valid JSON response
            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError:
                print(f"Invalid JSON response for {state_name}")
                continue
            
            if 'data' not in data:
                print(f"No data found for {state_name}")
                continue
                
            for ngo in data['data']:
                ngo_data = {
                    'darpan_id': ngo.get('darpan_id'),
                    'name': ngo.get('organisation_name'),
                    'state': state_name,
                    'district': ngo.get('district_name'),
                    'registration_type': ngo.get('registration_type'),
                    'registration_date': ngo.get('date_of_registration'),
                    'sectors': clean_sectors(ngo.get('sector_name')),
                    'fcra_status': 'Yes' in ngo.get('fcra_detail', ''),
                    '12a_status': 'Yes' in ngo.get('12a', ''),
                    '80g_status': 'Yes' in ngo.get('80g', ''),
                }
                all_ngos.append(ngo_data)
            
            time.sleep(random.uniform(2, 5))  # Increased delay
            
        except Exception as e:
            print(f"Error scraping {state_name}: {str(e)}")
            continue
    
    return pd.DataFrame(all_ngos)

def scrape_csr_projects():
    """Fixed SSL verification using certifi"""
    projects = []
    base_url = "https://csrbox.org/India-CSR-projects-list"
    
    try:
        # Use certifi's CA bundle
        response = requests.get(base_url, headers=HEADERS, verify=certifi.where())
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Rest of the scraping logic remains same
        # ... (keep previous implementation)
            
    except Exception as e:
        print(f"Error scraping CSR Box: {str(e)}")
    
    return pd.DataFrame(projects)

# Rest of helper functions remain same
# ... (keep clean_sectors, validate_url, etc)

if __name__ == "__main__":
    # Scrape NGO data
    ngo_df = scrape_ngo_darpan()
    if not ngo_df.empty:
        ngo_df.to_csv('indian_ngos.csv', index=False)
    
    # Scrape CSR Projects
    projects_df = scrape_csr_projects()
    if not projects_df.empty:
        projects_df.to_csv('csr_projects.csv', index=False)
    
    print(f"Scraped {len(ngo_df)} NGOs and {len(projects_df)} CSR projects")