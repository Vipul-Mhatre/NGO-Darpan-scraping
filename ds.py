import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from tqdm import tqdm
import re

# Configure headers to mimic browser behavior
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7'
}

# Indian state codes mapped to NGO Darpan parameters
INDIAN_STATES = {
    'MH': 'Maharashtra',
    'DL': 'Delhi',
    'KA': 'Karnataka',
    # Add all 28 states and 8 UTs
}

def scrape_ngo_darpan():
    """Scrape NGO data from https://ngodarpan.gov.in"""
    base_url = "https://ngodarpan.gov.in/index.php/ajaxcontroller/get_ajxdata"
    
    all_ngos = []
    
    for state_code, state_name in tqdm(INDIAN_STATES.items(), desc="Scraping States"):
        try:
            payload = {
                'state_id': state_code,
                'per_page': 10000,  # Max allowed
                'page': 0
            }
            
            response = requests.post(base_url, headers=HEADERS, data=payload)
            data = response.json()['data']
            
            for ngo in data:
                # Extract key compliance parameters
                ngo_data = {
                    'darpan_id': ngo.get('darpan_id'),
                    'name': ngo.get('organisation_name'),
                    'state': state_name,
                    'district': ngo.get('district_name'),
                    'registration_type': ngo.get('registration_type'),
                    'registration_date': pd.to_datetime(ngo.get('date_of_registration')),
                    'sectors': clean_sectors(ngo.get('sector_name')),
                    'fcra_status': 'Yes' in ngo.get('fcra_detail'),
                    '12a_status': 'Yes' in ngo.get('12a'),
                    '80g_status': 'Yes' in ngo.get('80g'),
                    'contact': re.sub(r'\D', '', ngo.get('mobile'))[-10:],
                    'website': validate_url(ngo.get('organisation_website'))
                }
                all_ngos.append(ngo_data)
            
            time.sleep(random.uniform(1, 3))  # Respect rate limits
            
        except Exception as e:
            print(f"Error scraping {state_name}: {str(e)}")
            continue
    
    return pd.DataFrame(all_ngos)

def clean_sectors(raw_sectors: str) -> list:
    """Map NGO sectors to Schedule VII categories"""
    schedule_vii_mapping = {
        'Education': ['education', 'school', 'literacy'],
        'Healthcare': ['health', 'hospital', 'medical'],
        'Environment': ['environment', 'climate', 'forest'],
        # Complete mapping per Schedule VII of Companies Act
    }
    
    matched_sectors = []
    for category, keywords in schedule_vii_mapping.items():
        if any(kw in raw_sectors.lower() for kw in keywords):
            matched_sectors.append(category)
    
    return matched_sectors if matched_sectors else ['Other']

def validate_url(url: str) -> str:
    """Sanitize website URLs"""
    if pd.isna(url) or url.strip() in ('', 'NA'):
        return ''
    return url if url.startswith('http') else f'http://{url}'

def scrape_csr_projects():
    """Scrape CSR projects from CSR Box"""
    projects = []
    base_url = "https://csrbox.org/India-CSR-projects-list"
    
    try:
        response = requests.get(base_url, headers=HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Pagination handling
        pages = int(soup.find('div', class_='pagination').find_all('a')[-2].text)
        
        for page in tqdm(range(1, pages+1), desc="Scraping CSR Projects"):
            page_url = f"{base_url}?page={page}"
            page_response = requests.get(page_url, headers=HEADERS)
            page_soup = BeautifulSoup(page_response.content, 'html.parser')
            
            for card in page_soup.find_all('div', class_='project-card'):
                project = {
                    'company': card.find('h3').text.strip(),
                    'project_title': card.find('h4').text.strip(),
                    'location': extract_location(card.find('p', class_='location').text),
                    'sectors': [tag.text.strip() for tag in card.find_all('span', class_='sector-tag')],
                    'budget': convert_budget(card.find('div', class_='budget').text),
                    'duration': card.find('div', class_='duration').text,
                    'sdgs': extract_sdgs(card.find('div', class_='sdgs').text)
                }
                projects.append(project)
            
            time.sleep(random.uniform(1.5, 4))
            
    except Exception as e:
        print(f"Error scraping CSR Box: {str(e)}")
    
    return pd.DataFrame(projects)

def extract_location(location_str: str) -> dict:
    """Extract state and district from location string"""
    parts = location_str.split(',')
    return {
        'district': parts[0].strip(),
        'state': parts[-1].strip() if len(parts) > 1 else ''
    }

def convert_budget(budget_str: str) -> float:
    """Convert budget strings to numeric values"""
    multipliers = {'L': 1e5, 'Cr': 1e7}
    value = re.findall(r'[\d.]+', budget_str)
    unit = re.findall(r'[A-Za-z]+', budget_str)
    
    if value and unit:
        return float(value[0]) * multipliers.get(unit[0], 1)
    return 0.0

def extract_sdgs(sdg_text: str) -> list:
    """Extract UN SDG numbers from text"""
    return list(set(re.findall(r'\b\d{1,2}\b', sdg_text)))

if __name__ == "__main__":
    # Scrape NGO data
    ngo_df = scrape_ngo_darpan()
    ngo_df.to_csv('indian_ngos.csv', index=False)
    
    # Scrape CSR Projects
    projects_df = scrape_csr_projects()
    projects_df.to_csv('csr_projects.csv', index=False)
    
    print(f"Scraped {len(ngo_df)} NGOs and {len(projects_df)} CSR projects")