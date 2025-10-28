from flask import Flask, request, jsonify
import sqlite3, os
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Always resolve path relative to this file's folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "mgnrega.db")

def query_db(query, params=()):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
        return [dict(row) for row in rows]

@app.route("/")
def home():
    return jsonify({"message": "Flask server connected"})

@app.route("/districts")
def get_districts():
    state = request.args.get("state")
    if not state:
        return jsonify({"error": "Missing state parameter"}), 400
    rows = query_db("SELECT DISTINCT District_Name FROM mgnrega_data WHERE State_Name = ? ORDER BY District_Name", (state,))
    return jsonify([r["District_Name"] for r in rows])

@app.route("/fin-years")
def get_fin_years():
    district = request.args.get("district")
    if not district:
        return jsonify({"error": "Missing district parameter"}), 400
    rows = query_db("SELECT DISTINCT fin_year FROM mgnrega_data WHERE District_Name = ? ORDER BY fin_year DESC", (district,))
    return jsonify([r["fin_year"] for r in rows])

@app.route("/data")
def get_data():
    state = request.args.get("state")
    district = request.args.get("district")
    fin_year = request.args.get("fin_year")
    if not (state and district):
        return jsonify({"error": "Missing parameters"}), 400
    if fin_year:
        rows = query_db("SELECT * FROM mgnrega_data WHERE State_Name = ? AND District_Name = ? AND fin_year = ? ORDER BY Month", (state, district, fin_year))
    else:
        rows = query_db("SELECT * FROM mgnrega_data WHERE State_Name = ? AND District_Name = ? ORDER BY fin_year DESC, Month", (state, district))
    return jsonify(rows)

@app.route("/all-districts")
def get_all_districts():
    state = request.args.get("state")
    if not state:
        return jsonify({"error": "Missing state parameter"}), 400
    rows = query_db("SELECT * FROM mgnrega_data WHERE State_Name = ? ORDER BY fin_year DESC, District_Name", (state,))
    return jsonify(rows)

@app.route("/states")
def get_states():
    rows = query_db("SELECT DISTINCT State_Name FROM mgnrega_data ORDER BY State_Name")
    return jsonify([r["State_Name"] for r in rows])

# <CHANGE> New endpoint to map city to district
@app.route("/city-to-district")
def city_to_district():
    city = request.args.get("city")
    if not city:
        return jsonify({"error": "Missing city parameter"}), 400
    
    # Search for city in district_city table (case-insensitive)
    rows = query_db("SELECT District FROM district_city WHERE UPPER(City) = ?", (city.upper(),))
    
    if rows and len(rows) > 0:
        mapped_district = rows[0]["District"]
        
        # Verify if this district exists in mgnrega_data
        district_exists = query_db("SELECT DISTINCT District_Name FROM mgnrega_data WHERE UPPER(District_Name) = ?", (mapped_district.upper(),))
        
        if district_exists and len(district_exists) > 0:
            return jsonify({
                "success": True,
                "district": district_exists[0]["District_Name"],
                "city": city
            })
        else:
            return jsonify({
                "success": False,
                "message": "District found in mapping but not in MGNREGA data",
                "mapped_district": mapped_district
            })
    else:
        return jsonify({
            "success": False,
            "message": "City not found in district_city mapping"
        })

if __name__ == "__main__":
    app.run(debug=True)