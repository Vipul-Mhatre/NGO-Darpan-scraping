import json

class MatchingEngine:
    def __init__(self, data_collector):
        self.data_collector = data_collector
        self.conn = data_collector.conn
    
    def get_ngo_by_id(self, darpan_id):
        """Retrieve NGO data from database by Darpan ID"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM ngos WHERE darpan_id = ?", (darpan_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        columns = [desc[0] for desc in cursor.description]
        ngo = dict(zip(columns, row))
        
        # Parse JSON fields
        for field in ['focus_areas', 'sdgs', 'schedule_vii_categories']:
            if ngo.get(field):
                ngo[field] = json.loads(ngo[field])
        
        return ngo
    
    def get_company_by_cin(self, cin):
        """Retrieve company data from database by CIN"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM companies WHERE cin = ?", (cin,))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        columns = [desc[0] for desc in cursor.description]
        company = dict(zip(columns, row))
        
        # Parse JSON fields
        for field in ['preferred_geographies', 'focus_areas', 'sdgs', 'compliance_requirements']:
            if company.get(field):
                company[field] = json.loads(company[field])
        
        return company
    
    def verify_compliance(self, ngo):
        """Check MCA-mandated requirements for NGOs"""
        compliance_status = {
            "is_compliant": False,
            "issues": []
        }
        
        # Check for required registrations
        if not ngo.get('has_12a'):
            compliance_status["issues"].append("Missing 12A registration")
        
        if not ngo.get('has_80g'):
            compliance_status["issues"].append("Missing 80G registration")
        
        # Check for CSR-1 registration (required since 2021)
        # This would typically be checked via API, but we're simulating here
        if not ngo.get('has_csr1', False):
            compliance_status["issues"].append("Missing CSR-1 registration")
        
        # Check for FCRA if working with foreign funds
        if not ngo.get('has_fcra') and ngo.get('works_with_foreign_funds', False):
            compliance_status["issues"].append("Missing FCRA registration for foreign funds")
        
        # Check credibility score
        if ngo.get('credibility_score', 0) < 3:
            compliance_status["issues"].append("Low credibility score")
        
        compliance_status["is_compliant"] = len(compliance_status["issues"]) == 0
        return compliance_status
    
    def calculate_geographic_proximity(self, company_locations, ngo_location):
        """Calculate geographic proximity score between company and NGO"""
        # Convert location strings to standardized format
        if not company_locations or not ngo_location:
            return 0
            
        # Get state and district from NGO location
        ngo_state = ngo_location.get('state', '').lower()
        ngo_district = ngo_location.get('district', '').lower()
        
        # Check for exact matches in company's preferred locations
        for location in company_locations:
            company_state = location.get('state', '').lower()
            company_district = location.get('district', '').lower()
            
            # Exact district match
            if company_district and ngo_district and company_district == ngo_district:
                return 100
            
            # State match
            if company_state and ngo_state and company_state == ngo_state:
                return 75
            
            # Check for aspirational district match
            if ngo_district in self.get_aspirational_districts():
                return 85
        
        # No match found
        return 25
    
    def get_aspirational_districts(self):
        """Return list of aspirational districts as defined by NITI Aayog"""
        # This would typically come from an API or database
        # Simplified list for demonstration
        return [
            "kishanganj", "araria", "begusarai", "sheikhpura", "gaya", "muzaffarpur",
            "purnia", "katihar", "aurangabad", "banka", "sitamarhi", "nawada",
            "jamui", "khagaria", "purbi champaran", "darbhanga", "bastar",
            "bijapur", "dantewada", "kanker", "kondagaon", "narayanpur",
            "rajnandgaon", "sukma", "dahod", "narmada", "baksa", "barpeta",
            "darrang", "dhubri", "goalpara", "hailakandi", "udalguri"
        ]
    
    def calculate_sdg_alignment(self, company_sdgs, ngo_sdgs):
        """Calculate alignment score between company and NGO SDGs"""
        if not company_sdgs or not ngo_sdgs:
            return 0
            
        # Convert to sets for intersection calculation
        company_sdg_set = set(company_sdgs)
        ngo_sdg_set = set(ngo_sdgs)