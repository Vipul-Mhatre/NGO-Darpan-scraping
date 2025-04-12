from flask import Flask, render_template, request
import json

app = Flask(__name__)

def load_ngo_data():
    with open('data/ngo.json', 'r') as f:
        data = json.load(f)
    return data

@app.route('/', methods=['GET'])
def index():
    ngos = load_ngo_data()
    search_query = request.args.get('search', '').lower()
    district = request.args.get('district', '')
    
    # Get unique districts for filter dropdown
    districts = set()
    for ngo in ngos:
        if 'Key Issues' in ngo and 'Operational Area-District' in ngo['Key Issues']:
            districts_str = ngo['Key Issues']['Operational Area-District']
            if districts_str and districts_str != "Not Available":
                for d in districts_str.split(','):
                    if '->' in d:
                        state, dist = d.split('->')
                        districts.add(dist.strip())

    # Filter NGOs based on search and district
    if search_query or district:
        filtered_ngos = []
        for ngo in ngos:
            name = ngo.get('name', '').lower()
            achievements = ngo.get('Details of Achievements', '').lower()
            ngo_district = ngo.get('Key Issues', {}).get('Operational Area-District', '')
            
            if search_query in name or search_query in achievements:
                if district and district in ngo_district:
                    filtered_ngos.append(ngo)
                elif not district:
                    filtered_ngos.append(ngo)
        ngos = filtered_ngos

    return render_template('index.html', ngos=ngos, districts=sorted(districts))

if __name__ == '__main__':
    app.run(debug=True)
