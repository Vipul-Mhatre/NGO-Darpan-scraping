import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
import time
from datetime import datetime, timedelta
from geopy.distance import geodesic
from fuzzywuzzy import fuzz, process
import sqlite3
import logging
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='csr_matchmaker.log'
)
logger = logging.getLogger('csr_matchmaker')

# Load environment variables
load_dotenv()
MCA_API_KEY = os.getenv('MCA_API_KEY')
DARPAN_API_KEY = os.getenv('DARPAN_API_KEY')

class DataCollector:
    def __init__(self, cache_dir="./cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"Initialized DataCollector with cache directory: {cache_dir}")
        
        # Initialize database connection
        self.conn = sqlite3.connect('csr_matchmaker.db')
        logger.info("Connected to database: csr_matchmaker.db")
        self.create_tables()
        
        # Initialize Selenium WebDriver
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=chrome_options)
        
    def create_tables(self):
        """Create necessary database tables if they don't exist"""
        logger.info("Creating/verifying database tables")
        cursor = self.conn.cursor()
        try:
            # NGO table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ngos (
                darpan_id TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                district TEXT,
                pincode TEXT,
                focus_areas TEXT,
                sdgs TEXT,
                schedule_vii_categories TEXT,
                has_12a INTEGER,
                has_80g INTEGER,
                has_fcra INTEGER,
                annual_budget REAL,
                csr_funds_utilized REAL,
                credibility_score REAL,
                last_updated TIMESTAMP
            )
            ''')
            
            # Company table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                cin TEXT PRIMARY KEY,
                name TEXT,
                csr_budget REAL,
                preferred_geographies TEXT,
                focus_areas TEXT,
                sdgs TEXT,
                compliance_requirements TEXT,
                preferred_ngo_size TEXT,
                last_updated TIMESTAMP
            )
            ''')
            
            # Matches table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_cin TEXT,
                ngo_darpan_id TEXT,
                match_score REAL,
                strengths TEXT,
                compliance_status TEXT,
                risk_factors TEXT,
                created_at TIMESTAMP,
                FOREIGN KEY (company_cin) REFERENCES companies (cin),
                FOREIGN KEY (ngo_darpan_id) REFERENCES ngos (darpan_id)
            )
            ''')
            
            self.conn.commit()
            logger.info("Database tables created/verified successfully")
        except sqlite3.Error as e:
            logger.error(f"Database error during table creation: {e}")
            raise
    
    def fetch_ngo_darpan(self, state, force_refresh=False):
        """Fetch NGO data from NGO Darpan portal using web scraping"""
        logger.info(f"Scraping NGO Darpan data for state: {state}")
        cache_file = f"{self.cache_dir}/ngo_darpan_{state}.csv"
        
        if not force_refresh and os.path.exists(cache_file):
            file_time = os.path.getmtime(cache_file)
            if (time.time() - file_time) < 7 * 24 * 60 * 60:
                return pd.read_csv(cache_file)

        try:
            all_ngos = []
            base_url = "https://ngodarpan.gov.in/index.php/search/"
            
            # Initialize the browser session
            self.driver.get(base_url)
            time.sleep(3)  # Wait for page load
            
            # Select state from dropdown
            state_select = Select(self.driver.find_element(By.NAME, "state"))
            state_select.select_by_value(str(state))
            time.sleep(2)
            
            # Click search button
            search_button = self.driver.find_element(By.ID, "searchbtn")
            search_button.click()
            time.sleep(3)
            
            while True:
                try:
                    # Wait for table to load
                    table = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "ngo-table"))
                    )
                    
                    # Extract data from current page
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header
                    if not rows:
                        break
                        
                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 6:
                            ngo_data = {
                                'darpan_id': cols[0].text.strip(),
                                'name': cols[1].text.strip(),
                                'state': cols[2].text.strip(),
                                'district': cols[3].text.strip(),
                                'sector': cols[4].text.strip(),
                                'registration_no': cols[5].text.strip()
                            }
                            all_ngos.append(ngo_data)
                    
                    logger.info(f"Scraped {len(all_ngos)} NGOs so far...")
                    
                    # Check for next page
                    next_button = self.driver.find_element(By.CLASS_NAME, "next")
                    if "disabled" in next_button.get_attribute("class"):
                        break
                    next_button.click()
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error while scraping page: {str(e)}")
                    break

            if all_ngos:
                df = pd.DataFrame(all_ngos)
                df.to_csv(cache_file, index=False)
                logger.info(f"Successfully scraped and saved {len(df)} NGO records")
                return df
            
        except Exception as e:
            logger.error(f"Error during NGO scraping: {str(e)}", exc_info=True)
            if os.path.exists(cache_file):
                return pd.read_csv(cache_file)
            return pd.DataFrame()
            
        finally:
            self.driver.quit()

    def __del__(self):
        """Cleanup browser resources"""
        try:
            self.driver.quit()
        except:
            pass

    def fetch_guidestar_ratings(self, ngo_id):
        """Fetch NGO credibility ratings from GuideStar India"""
        logger.info(f"Fetching GuideStar ratings for NGO ID: {ngo_id}")
        cache_file = f"{self.cache_dir}/guidestar_{ngo_id}.json"
        
        if os.path.exists(cache_file):
            logger.info(f"Using cached GuideStar data for NGO ID: {ngo_id}")
            with open(cache_file, 'r') as f:
                return json.load(f)
        
        try:
            # This is a placeholder - actual implementation would use GuideStar API
            url = f"https://www.guidestarindia.org/api/ngo/{ngo_id}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
                logger.info(f"Successfully fetched GuideStar data for NGO ID: {ngo_id}")
                return data
            else:
                logger.error(f"Failed to fetch GuideStar data for NGO ID: {ngo_id}, HTTP {response.status_code}")
                return {"credibility_score": None}
                
        except Exception as e:
            logger.error(f"Error fetching GuideStar data for NGO ID: {ngo_id}: {str(e)}", exc_info=True)
            return {"credibility_score": None}
    
    def fetch_mca_company_data(self, cin):
        """Fetch company CSR data from MCA Portal"""
        logger.info(f"Fetching MCA company data for CIN: {cin}")
        cache_file = f"{self.cache_dir}/mca_{cin}.json"
        
        if os.path.exists(cache_file):
            file_time = os.path.getmtime(cache_file)
            if (time.time() - file_time) < 30 * 24 * 60 * 60:  # 30 days
                logger.info(f"Using cached MCA data for CIN: {cin}")
                with open(cache_file, 'r') as f:
                    return json.load(f)
        
        try:
            # This is a placeholder - actual implementation would use MCA API
            url = f"https://data.gov.in/api/mca/company/{cin}"
            headers = {"Authorization": f"Bearer {MCA_API_KEY}"}
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
                logger.info(f"Successfully fetched MCA data for CIN: {cin}")
                return data
            else:
                logger.error(f"Failed to fetch MCA data for CIN: {cin}, HTTP {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching MCA data for CIN: {cin}: {str(e)}", exc_info=True)
            return {}
    
    def scrape_csr_box(self, limit=100):
        """Scrape CSR Box for project data"""
        logger.info(f"Scraping CSR Box for project data (limit: {limit})")
        cache_file = f"{self.cache_dir}/csrbox_projects.csv"
        
        if os.path.exists(cache_file):
            file_time = os.path.getmtime(cache_file)
            if (time.time() - file_time) < 14 * 24 * 60 * 60:  # 14 days
                logger.info(f"Using cached CSR Box data from {cache_file}")
                return pd.read_csv(cache_file)
        
        try:
            projects = []
            for page in range(1, 11):  # Scrape 10 pages
                url = f"https://csrbox.org/India-CSR-projects?page={page}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    project_cards = soup.select('.project-card')
                    
                    for card in project_cards:
                        project = {
                            'title': card.select_one('.project-title').text.strip(),
                            'company': card.select_one('.company-name').text.strip(),
                            'focus_area': card.select_one('.focus-area').text.strip(),
                            'location': card.select_one('.location').text.strip(),
                            'budget': card.select_one('.budget').text.strip(),
                        }
                        projects.append(project)
                        
                        if len(projects) >= limit:
                            break
                
                time.sleep(2)  # Be respectful with scraping
            
            df = pd.DataFrame(projects)
            df.to_csv(cache_file, index=False)
            logger.info(f"Successfully scraped and cached {len(df)} CSR Box projects")
            return df
            
        except Exception as e:
            logger.error(f"Error scraping CSR Box: {str(e)}", exc_info=True)
            if os.path.exists(cache_file):
                return pd.read_csv(cache_file)
            return pd.DataFrame()
    
    def map_sdgs_to_schedule_vii(self, sdgs):
        """Map UN SDGs to Schedule VII categories of Companies Act 2013"""
        logger.info(f"Mapping SDGs to Schedule VII categories: {sdgs}")
        mapping = {
            1: ["poverty eradication", "hunger eradication"],  # No Poverty
            2: ["hunger eradication", "agriculture"], # Zero Hunger
            3: ["healthcare", "preventive healthcare"], # Good Health
            4: ["education", "vocational skills"], # Quality Education
            5: ["gender equality", "women empowerment"], # Gender Equality
            6: ["sanitation", "safe drinking water"], # Clean Water
            7: ["renewable energy"], # Affordable and Clean Energy
            8: ["employment", "vocational skills", "livelihood"], # Decent Work
            9: ["innovation", "technology incubators"], # Industry, Innovation
            10: ["socio-economic inequalities", "marginalized groups"], # Reduced Inequalities
            11: ["slum development", "housing"], # Sustainable Cities
            12: ["sustainable consumption"], # Responsible Consumption
            13: ["environmental sustainability", "ecological balance"], # Climate Action
            14: ["marine resources", "conservation"], # Life Below Water
            15: ["forest conservation", "biodiversity"], # Life on Land
            16: ["peace", "justice", "governance"], # Peace, Justice
            17: ["public-private partnerships"] # Partnerships for the Goals
        }
        
        schedule_vii_categories = []
        for sdg in sdgs:
            if isinstance(sdg, int) and 1 <= sdg <= 17:
                schedule_vii_categories.extend(mapping.get(sdg, []))
        
        logger.info(f"Mapped SDGs to Schedule VII categories: {schedule_vii_categories}")
        return list(set(schedule_vii_categories))
    
    def store_ngo_data(self, ngo_data):
        """Store processed NGO data in the database"""
        logger.info(f"Storing {len(ngo_data)} NGO records in database")
        cursor = self.conn.cursor()
        stored_count = 0
        error_count = 0
        
        for _, ngo in ngo_data.iterrows():
            try:
                sdgs = json.dumps(ngo.get('sdgs', []))
                schedule_vii = json.dumps(ngo.get('schedule_vii_categories', []))
                focus_areas = json.dumps(ngo.get('focus_areas', []))
                
                cursor.execute('''
                INSERT OR REPLACE INTO ngos 
                (darpan_id, name, state, district, pincode, focus_areas, sdgs, 
                schedule_vii_categories, has_12a, has_80g, has_fcra, 
                annual_budget, csr_funds_utilized, credibility_score, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ngo.get('darpan_id'), 
                    ngo.get('name'),
                    ngo.get('state'),
                    ngo.get('district'),
                    ngo.get('pincode'),
                    focus_areas,
                    sdgs,
                    schedule_vii,
                    int(ngo.get('has_12a', False)),
                    int(ngo.get('has_80g', False)),
                    int(ngo.get('has_fcra', False)),
                    ngo.get('annual_budget'),
                    ngo.get('csr_funds_utilized'),
                    ngo.get('credibility_score'),
                    datetime.now().isoformat()
                ))
                stored_count += 1
            except sqlite3.Error as e:
                logger.error(f"Error storing NGO {ngo.get('darpan_id')}: {e}")
                error_count += 1
        
        self.conn.commit()
        logger.info(f"Stored {stored_count} NGO records successfully, {error_count} errors")
    
    def store_company_data(self, company_data):
        """Store processed company data in the database"""
        logger.info(f"Storing {len(company_data)} company records in database")
        cursor = self.conn.cursor()
        stored_count = 0
        error_count = 0
        
        for _, company in company_data.iterrows():
            try:
                preferred_geographies = json.dumps(company.get('preferred_geographies', []))
                focus_areas = json.dumps(company.get('focus_areas', []))
                sdgs = json.dumps(company.get('sdgs', []))
                compliance_requirements = json.dumps(company.get('compliance_requirements', []))
                
                cursor.execute('''
                INSERT OR REPLACE INTO companies 
                (cin, name, csr_budget, preferred_geographies, focus_areas, sdgs,
                compliance_requirements, preferred_ngo_size, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    company.get('cin'),
                    company.get('name'),
                    company.get('csr_budget'),
                    preferred_geographies,
                    focus_areas,
                    sdgs,
                    compliance_requirements,
                    company.get('preferred_ngo_size'),
                    datetime.now().isoformat()
                ))
                stored_count += 1
            except sqlite3.Error as e:
                logger.error(f"Error storing company {company.get('cin')}: {e}")
                error_count += 1
        
        self.conn.commit()
        logger.info(f"Stored {stored_count} company records successfully, {error_count} errors")