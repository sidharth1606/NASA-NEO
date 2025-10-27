import streamlit as st
import mysql.connector
from datetime import datetime
import csv
from io import StringIO
import pandas as pd
import requests
import json

# DB connection
db_connected = False
try:
    conn = mysql.connector.connect(
        host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
        user="34EjyZiSa86ATwm.root",
        password="rliXje9olqc9Q1Re",
        port=4000,
        database="nasa_neo_tracking"
    )
    cursor = conn.cursor()
    db_connected = True
except mysql.connector.Error as err:
    st.warning(f"Database connection failed: {err}. Using sample data for demonstration.")
    db_connected = False

# Mock data functions
def get_mock_query_results(query_name):
    mock_data = {
        "Count of Asteroid Approaches": [("12345", 5), ("67890", 3)],
        "Average Velocity of Each Asteroid": [("12345", 25000.5), ("67890", 30000.2)],
        "Top 10 Fastest Asteroids": [("Asteroid A", 50000), ("Asteroid B", 45000)],
        "Hazardous Asteroids Approached >3 Times": [("Hazardous A", 4)],
        "Month with Most Approaches": [(6, 150)],
        "Asteroid with Fastest Approach Speed": [("Fast Asteroid", 60000)],
        "Asteroids by Max Diameter (Descending)": [("Large Asteroid", 10.5), ("Medium Asteroid", 5.2)],
        "Asteroids Getting Nearer Over Time": [("Asteroid X", "2024-01-01", 1000000), ("Asteroid Y", "2024-02-01", 900000)],
        "Closest Approach Details": [("Close Asteroid", "2024-03-01", 500000), ("Closer Asteroid", "2024-04-01", 300000)],
        "Asteroids with Velocity >50,000 km/h": [("Fast One",)],
        "Approaches per Month": [(1, 50), (2, 60), (3, 70)],
        "Asteroid with Highest Brightness": [("Bright Asteroid", 15.2)],
        "Hazardous vs Non-Hazardous Count": [(0, 800), (1, 200)],
        "Asteroids Closer Than Moon": [("Moon Close", "2024-05-01", 0.8)],
        "Asteroids Within 0.05 AU": [("AU Close", "2024-06-01", 0.03)]
    }
    return mock_data.get(query_name, [])

def get_mock_filtered_data():
    return [
        ("Asteroid 1", "2024-01-01", 25000, 0.5, 1000000, 2.5, 0.5, 1.0, 0),
        ("Asteroid 2", "2024-02-01", 30000, 0.3, 800000, 2.0, 0.3, 0.8, 1)
    ]

def get_mock_metrics():
    return {"total_asteroids": 1000, "hazardous_count": 200, "total_approaches": 1500}

def get_mock_pie_data():
    return [("Non-Hazardous", 800), ("Hazardous", 200)]

def get_mock_month_data():
    return [("Month 1", 50), ("Month 2", 60), ("Month 3", 70)]

st.title("🛰️ NASA Near-Earth Object (NEO) Tracking & Insights")

# Sidebar for queries
st.sidebar.header("Select Query")
queries = {
    "Count of Asteroid Approaches": """
        SELECT neo_reference_id, COUNT(*) AS approach_count
        FROM close_approach
        GROUP BY neo_reference_id
        ORDER BY approach_count DESC;
    """,
    "Average Velocity of Each Asteroid": """
        SELECT neo_reference_id, AVG(relative_velocity_kmph) AS avg_velocity
        FROM close_approach
        GROUP BY neo_reference_id
        ORDER BY avg_velocity DESC;
    """,
    "Top 10 Fastest Asteroids": """
        SELECT a.name, c.relative_velocity_kmph
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        ORDER BY c.relative_velocity_kmph DESC
        LIMIT 10;
    """,
    "Hazardous Asteroids Approached >3 Times": """
        SELECT a.name, COUNT(*) AS count
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        WHERE a.is_potentially_hazardous_asteroid = 1
        GROUP BY c.neo_reference_id
        HAVING count > 3;
    """,
    "Month with Most Approaches": """
        SELECT MONTH(close_approach_date) AS month, COUNT(*) AS count
        FROM close_approach
        GROUP BY month
        ORDER BY count DESC
        LIMIT 1;
    """,
    "Asteroid with Fastest Approach Speed": """
        SELECT a.name, c.relative_velocity_kmph
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        ORDER BY c.relative_velocity_kmph DESC
        LIMIT 1;
    """,
    "Asteroids by Max Diameter (Descending)": """
        SELECT name, estimated_diameter_max_km
        FROM asteroids
        ORDER BY estimated_diameter_max_km DESC;
    """,
    "Asteroids Getting Nearer Over Time": """
        SELECT a.name, c.close_approach_date, c.miss_distance_km
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        ORDER BY c.close_approach_date, c.miss_distance_km;
    """,
    "Closest Approach Details": """
        SELECT a.name, c.close_approach_date, c.miss_distance_km
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        ORDER BY c.miss_distance_km ASC
        LIMIT 10;
    """,
    "Asteroids with Velocity >50,000 km/h": """
        SELECT a.name
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        WHERE c.relative_velocity_kmph > 50000;
    """,
    "Approaches per Month": """
        SELECT MONTH(close_approach_date) AS month, COUNT(*) AS count
        FROM close_approach
        GROUP BY month
        ORDER BY month;
    """,
    "Asteroid with Highest Brightness": """
        SELECT name, absolute_magnitude_h
        FROM asteroids
        ORDER BY absolute_magnitude_h ASC
        LIMIT 1;
    """,
    "Hazardous vs Non-Hazardous Count": """
        SELECT is_potentially_hazardous_asteroid, COUNT(*) AS count
        FROM asteroids
        GROUP BY is_potentially_hazardous_asteroid;
    """,
    "Asteroids Closer Than Moon": """
        SELECT a.name, c.close_approach_date, c.miss_distance_lunar
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        WHERE c.miss_distance_lunar < 1;
    """,
    "Asteroids Within 0.05 AU": """
        SELECT a.name, c.close_approach_date, c.astronomical
        FROM close_approach c
        JOIN asteroids a ON c.neo_reference_id = a.id
        WHERE c.astronomical < 0.05;
    """
}

selected_query = st.sidebar.selectbox("Choose a query:", list(queries.keys()))

# Filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", value=datetime(2024, 1, 1))
end_date = st.sidebar.date_input("End Date", value=datetime(2024, 12, 31))
au_min = st.sidebar.slider("Min Astronomical Units", 0.0, 1.0, 0.0)
au_max = st.sidebar.slider("Max Astronomical Units", 0.0, 1.0, 1.0)
lunar_min = st.sidebar.slider("Min Lunar Distance", 0.0, 10.0, 0.0)
lunar_max = st.sidebar.slider("Max Lunar Distance", 0.0, 10.0, 10.0)
velocity_min = st.sidebar.slider("Min Velocity (km/h)", 0, 100000, 0)
velocity_max = st.sidebar.slider("Max Velocity (km/h)", 0, 100000, 100000)
dia_min = st.sidebar.slider("Min Diameter (km)", 0.0, 10.0, 0.0)
dia_max = st.sidebar.slider("Max Diameter (km)", 0.0, 10.0, 10.0)
hazardous = st.sidebar.selectbox("Hazardous", ["All", "Yes", "No"])

# Build filter query
filter_conditions = []
if start_date and end_date:
    filter_conditions.append(f"c.close_approach_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'")
if au_min != 0.0 or au_max != 1.0:
    filter_conditions.append(f"c.astronomical BETWEEN {au_min} AND {au_max}")
if lunar_min != 0.0 or lunar_max != 10.0:
    filter_conditions.append(f"c.miss_distance_lunar BETWEEN {lunar_min} AND {lunar_max}")
if velocity_min != 0 or velocity_max != 100000:
    filter_conditions.append(f"c.relative_velocity_kmph BETWEEN {velocity_min} AND {velocity_max}")
if dia_min != 0.0 or dia_max != 10.0:
    filter_conditions.append(f"a.estimated_diameter_min_km >= {dia_min} AND a.estimated_diameter_max_km <= {dia_max}")
if hazardous != "All":
    haz_val = 1 if hazardous == "Yes" else 0
    filter_conditions.append(f"a.is_potentially_hazardous_asteroid = {haz_val}")

filter_str = " AND ".join(filter_conditions) if filter_conditions else "1=1"

# Main query with filters
main_query = f"""
    SELECT a.name, c.close_approach_date, c.relative_velocity_kmph, c.astronomical, c.miss_distance_km, c.miss_distance_lunar, a.estimated_diameter_min_km, a.estimated_diameter_max_km, a.is_potentially_hazardous_asteroid
    FROM close_approach c
    JOIN asteroids a ON c.neo_reference_id = a.id
    WHERE {filter_str}
    ORDER BY c.close_approach_date ASC;
"""

# Tabs
tab1, tab2, tab3 = st.tabs(["Query Results", "Filtered Data", "Visualizations"])

with tab1:
    st.header("Query Results")
    if selected_query:
        if db_connected:
            cursor.execute(queries[selected_query])
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        else:
            result = get_mock_query_results(selected_query)
            # Define columns based on query type
            if "Count of Asteroid Approaches" in selected_query:
                columns = ["neo_reference_id", "approach_count"]
            elif "Average Velocity" in selected_query:
                columns = ["neo_reference_id", "avg_velocity"]
            elif "Top 10 Fastest" in selected_query:
                columns = ["name", "relative_velocity_kmph"]
            elif "Hazardous Asteroids" in selected_query:
                columns = ["name", "count"]
            elif "Month with Most" in selected_query:
                columns = ["month", "count"]
            elif "Fastest Approach Speed" in selected_query:
                columns = ["name", "relative_velocity_kmph"]
            elif "Max Diameter" in selected_query:
                columns = ["name", "estimated_diameter_max_km"]
            elif "Getting Nearer" in selected_query:
                columns = ["name", "close_approach_date", "miss_distance_km"]
            elif "Closest Approach" in selected_query:
                columns = ["name", "close_approach_date", "miss_distance_km"]
            elif "Velocity >50,000" in selected_query:
                columns = ["name"]
            elif "Approaches per Month" in selected_query:
                columns = ["month", "count"]
            elif "Highest Brightness" in selected_query:
                columns = ["name", "absolute_magnitude_h"]
            elif "Hazardous vs Non-Hazardous" in selected_query:
                columns = ["is_potentially_hazardous_asteroid", "count"]
            elif "Closer Than Moon" in selected_query:
                columns = ["name", "close_approach_date", "miss_distance_lunar"]
            elif "Within 0.05 AU" in selected_query:
                columns = ["name", "close_approach_date", "astronomical"]
            else:
                columns = ["Column1", "Column2"]
        # Create table data without pandas, convert all to strings for compatibility
        table_data = [columns] + [[str(item) for item in row] for row in result]
        st.subheader(f"Results for: {selected_query}")
        st.table(table_data)
        if 'approach_count' in columns and result:
            # Simple bar chart data
            chart_data = {row[0]: row[1] for row in result[:10]}
            st.bar_chart(chart_data)
        # Export to CSV
        if st.button("Download Query Results as CSV"):
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(result)
            st.download_button(
                label="Download CSV",
                data=output.getvalue(),
                file_name=f"{selected_query.replace(' ', '_')}.csv",
                mime="text/csv"
            )

with tab2:
    st.header("Filtered Data")
    if db_connected:
        cursor.execute(main_query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    else:
        result = get_mock_filtered_data()
        columns = ["name", "close_approach_date", "relative_velocity_kmph", "astronomical", "miss_distance_km", "miss_distance_lunar", "estimated_diameter_min_km", "estimated_diameter_max_km", "is_potentially_hazardous_asteroid"]
    # Convert all to strings for compatibility
    table_data = [columns] + [[str(item) for item in row] for row in result]
    st.table(table_data)
    # Export to CSV
    if st.button("Download Filtered Data as CSV"):
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        writer.writerows(result)
        st.download_button(
            label="Download CSV",
            data=output.getvalue(),
            file_name="filtered_data.csv",
            mime="text/csv"
        )

with tab3:
    st.header("Visualizations")
    # Metrics
    col1, col2, col3 = st.columns(3)
    if db_connected:
        cursor.execute("SELECT COUNT(*) FROM asteroids")
        total_asteroids = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM asteroids WHERE is_potentially_hazardous_asteroid = 1")
        hazardous_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM close_approach")
        total_approaches = cursor.fetchone()[0]
    else:
        metrics = get_mock_metrics()
        total_asteroids = metrics["total_asteroids"]
        hazardous_count = metrics["hazardous_count"]
        total_approaches = metrics["total_approaches"]
    col1.metric("Total Asteroids", total_asteroids)
    col2.metric("Hazardous Asteroids", hazardous_count)
    col3.metric("Total Approaches", total_approaches)

    # Pie chart for hazardous vs non-hazardous
    if db_connected:
        cursor.execute("SELECT is_potentially_hazardous_asteroid, COUNT(*) FROM asteroids GROUP BY is_potentially_hazardous_asteroid")
        pie_data = cursor.fetchall()
    else:
        pie_data = get_mock_pie_data()
    pie_dict = {row[0]: row[1] for row in pie_data}
    st.subheader("Hazardous vs Non-Hazardous Asteroids")
    st.bar_chart(pie_dict)  # Using bar chart as pie alternative

    # Approaches per month
    if db_connected:
        cursor.execute("SELECT MONTH(close_approach_date), COUNT(*) FROM close_approach GROUP BY MONTH(close_approach_date) ORDER BY MONTH(close_approach_date)")
        month_data = cursor.fetchall()
    else:
        month_data = get_mock_month_data()
    month_dict = {row[0]: row[1] for row in month_data}
    st.subheader("Approaches per Month")
    st.line_chart(month_dict)

cursor.close()
conn.close()

# Data collection from NASA API
API_KEY = "M0oV1ghvmKnMqhS6c5ynHBzffBewcNx16KLNocYo"
target = 100
asteroids_data = []

# Starting URL
url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date=2024-01-01&end_date=2024-01-07&api_key={API_KEY}"

while len(asteroids_data) < target:
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        break
    data = response.json()

    neo_dates = data.get('near_earth_objects', {})

    for date, ast_list in neo_dates.items():
        for ast in ast_list:
            for approach in ast.get('close_approach_data', []):
                # Extract and clean data
                try:
                    asteroids_data.append({
                        'id': int(ast['id']),
                        'name': ast.get('name', 'Unknown'),
                        'magnitude': float(ast.get('absolute_magnitude_h', 0)),
                        'dia_min': float(ast['estimated_diameter']['kilometers']['estimated_diameter_min']),
                        'dia_max': float(ast['estimated_diameter']['kilometers']['estimated_diameter_max']),
                        'hazardous': ast.get('is_potentially_hazardous_asteroid', False),
                        'closest_approach_date': approach.get('close_approach_date', ''),
                        'velocity_kmph': float(approach['relative_velocity']['kilometers_per_hour']),
                        'astronomical': float(approach['miss_distance']['astronomical']),
                        'miss_distance_km': float(approach['miss_distance']['kilometers']),
                        'miss_distance_lunar': float(approach['miss_distance']['lunar']),
                        'orbiting_body': approach.get('orbiting_body', 'Earth')
                    })
                except (KeyError, ValueError) as e:
                    print(f"Skipping record due to error: {e}")
                    continue

                if len(asteroids_data) >= target:
                    break
            if len(asteroids_data) >= target:
                break
        if len(asteroids_data) >= target:
            break

    # Go to the next page
    if 'next' in data.get('links', {}):
        url = data['links']['next']
    else:
        break

print(f"✅ Total asteroids collected: {len(asteroids_data)}")

# Database connection for data insertion
conn = mysql.connector.connect(
    host="gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    user="34EjyZiSa86ATwm.root",
    password="rliXje9olqc9Q1Re",
    port=4000,
    database="nasa_neo_tracking"
)
cursor = conn.cursor()

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS asteroids (
    id INT,
    name VARCHAR(255),
    absolute_magnitude_h FLOAT,
    estimated_diameter_min_km FLOAT,
    estimated_diameter_max_km FLOAT,
    is_potentially_hazardous_asteroid BOOLEAN
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS close_approach (
    neo_reference_id INT,
    close_approach_date DATE,
    relative_velocity_kmph FLOAT,
    astronomical FLOAT,
    miss_distance_km FLOAT,
    miss_distance_lunar FLOAT,
    orbiting_body VARCHAR(255)
)
""")

# Insert data
for asteroid in asteroids_data:
    cursor.execute("""
    INSERT INTO asteroids (id, name, absolute_magnitude_h, estimated_diameter_min_km, estimated_diameter_max_km, is_potentially_hazardous_asteroid)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        asteroid['id'],
        asteroid['name'],
        asteroid['magnitude'],
        asteroid['dia_min'],
        asteroid['dia_max'],
        asteroid['hazardous']
    ))

    cursor.execute("""
    INSERT INTO close_approach (neo_reference_id, close_approach_date, relative_velocity_kmph, astronomical, miss_distance_km, miss_distance_lunar, orbiting_body)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        asteroid['id'],
        datetime.strptime(asteroid['closest_approach_date'], '%Y-%m-%d').date(),
        asteroid['velocity_kmph'],
        asteroid['astronomical'],
        asteroid['miss_distance_km'],
        asteroid['miss_distance_lunar'],
        asteroid['orbiting_body']
    ))

conn.commit()
cursor.close()
conn.close()

print("Data insertion completed.")
