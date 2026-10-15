from flask import Flask, render_template, jsonify, request
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import threading
import time
import os
from dotenv import load_dotenv

aload_dotenv()

DB_CONFIG = {
    "host": os.getenv('DB_HOST', 'localhost'),
    "dbname": os.getenv('DB_NAME', 'surveillance_store_db'),
    "user": os.getenv('DB_USER', 'admin'),
    "password": os.getenv('DB_PASSWORD', 'Admin123')
}

app = Flask(__name__)


class SurveillanceAPI:
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        """Initialize the PostgreSQL database"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create table for surveillance data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS surveillance_data (
                id SERIAL PRIMARY KEY,
                gerai_id VARCHAR(255) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                kamera_1 INTEGER NOT NULL,
                kamera_2 INTEGER NOT NULL,
                total_customer INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_gerai_timestamp ON surveillance_data(gerai_id, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON surveillance_data(timestamp)')
        
        conn.commit()
        conn.close()
    
    def get_connection(self):
        """Get database connection"""
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    
    def get_surveillance_data(self, start_date=None, end_date=None, store_id=None, limit=1000):
        """Get surveillance data with optional filters"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = "SELECT * FROM surveillance_data WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND DATE(timestamp) >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND DATE(timestamp) <= %s"
            params.append(end_date)
        
        if store_id and store_id != 'all':
            query += " AND gerai_id = %s"
            params.append(store_id)
        
        query += " ORDER BY timestamp DESC"
        
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        return [dict(row) for row in rows]
    
    def get_statistics(self, start_date=None, end_date=None, store_id=None):
        """Get dashboard statistics"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        conditions = "WHERE 1=1"
        params = []
        
        if start_date:
            conditions += " AND DATE(timestamp) >= %s"
            params.append(start_date)
        
        if end_date:
            conditions += " AND DATE(timestamp) <= %s"
            params.append(end_date)
            
        if store_id and store_id != 'all':
            conditions += " AND gerai_id = %s"
            params.append(store_id)
        
        # Total customers
        cursor.execute(f"SELECT SUM(total_customer) as total FROM surveillance_data {conditions}", params)
        total_customers = cursor.fetchone()['total'] or 0
        
        # Active stores
        cursor.execute(f"SELECT COUNT(DISTINCT gerai_id) as count FROM surveillance_data {conditions}", params)
        active_stores = cursor.fetchone()['count'] or 0
        
        # Average customers per hour
        cursor.execute(f"SELECT AVG(total_customer) as avg FROM surveillance_data {conditions}", params)
        avg_customers = cursor.fetchone()['avg'] or 0
        
        # Peak hour (most busy hour)
        peak_hour_query = f"""
            SELECT EXTRACT(HOUR FROM timestamp) as hour, SUM(total_customer) as total
            FROM surveillance_data {conditions}
            GROUP BY EXTRACT(HOUR FROM timestamp)
            ORDER BY total DESC
            LIMIT 1
        """
        cursor.execute(peak_hour_query, params)
        peak_hour_result = cursor.fetchone()
        peak_hour = f"{int(peak_hour_result['hour']):02d}:00" if peak_hour_result and peak_hour_result['hour'] else '--:--'
        
        conn.close()
        
        return {
            'total_customers': int(total_customers),
            'active_stores': int(active_stores),
            'avg_customers': round(float(avg_customers), 1),
            'peak_hour': peak_hour
        }
    
    def get_chart_data(self, view_type, start_date=None, end_date=None, store_id=None):
        """Get aggregated data for charts based on view type (hourly, daily, weekly)."""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        conditions = "WHERE 1=1"
        params = []

        if start_date:
            conditions += " AND DATE(timestamp) >= %s"
            params.append(start_date)
        if end_date:
            conditions += " AND DATE(timestamp) <= %s"
            params.append(end_date)
        if store_id and store_id != 'all':
            conditions += " AND gerai_id = %s"
            params.append(store_id)

        # Modified queries for each view type
        if view_type == 'hourly':
            query = """
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    gerai_id,
                    SUM(total_customer) as total_customers,
                    SUM(kamera_1) as total_kamera_1,
                    SUM(kamera_2) as total_kamera_2,
                    COUNT(*) as count
                FROM surveillance_data
                {conditions}
                GROUP BY DATE_TRUNC('hour', timestamp), gerai_id
                ORDER BY hour ASC
            """.format(conditions=conditions)
        elif view_type == 'daily':
            query = """
                SELECT 
                    DATE_TRUNC('day', timestamp) as day,
                    gerai_id,
                    SUM(total_customer) as total_customers,
                    SUM(kamera_1) as total_kamera_1,
                    SUM(kamera_2) as total_kamera_2,
                    COUNT(*) as count
                FROM surveillance_data
                {conditions}
                GROUP BY DATE_TRUNC('day', timestamp), gerai_id
                ORDER BY day ASC
            """.format(conditions=conditions)
        elif view_type == 'weekly':
            query = """
                SELECT 
                    DATE_TRUNC('week', timestamp) as week,
                    gerai_id,
                    SUM(total_customer) as total_customers,
                    SUM(kamera_1) as total_kamera_1,
                    SUM(kamera_2) as total_kamera_2,
                    COUNT(*) as count
                FROM surveillance_data
                {conditions}
                GROUP BY DATE_TRUNC('week', timestamp), gerai_id
                ORDER BY week ASC
            """.format(conditions=conditions)
        else:
            return []

        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries with proper timestamp formatting
            result = []
            for row in rows:
                data = dict(row)
                # Convert timestamp to ISO format for JavaScript
                for key in ['hour', 'day', 'week']:
                    if key in data and data[key] is not None:
                        data[key] = data[key].isoformat()
                result.append(data)
                
            return result
        except Exception as e:
            print(f"Error in get_chart_data: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def insert_surveillance_data(self, data):
        """Insert new surveillance data"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO surveillance_data (gerai_id, timestamp, kamera_1, kamera_2, total_customer)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            data['gerai_id'],
            data['timestamp'],
            data['kamera_1'],
            data['kamera_2'],
            data['total_customer']
        ))
        
        conn.commit()
        conn.close()

# Initialize API
api = SurveillanceAPI()

# --- Flask Routes ---

@app.route('/')
def dashboard():
    """Serve the dashboard HTML"""
    return render_template('dashboard.html')

@app.route('/api/data')
def get_data():
    """API endpoint to get raw surveillance data with filters"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date') or start_date 
    store_id = request.args.get('store_id', 'all')
    limit = request.args.get('limit', 1000, type=int)
    
    data = api.get_surveillance_data(start_date, end_date, store_id, limit)
    
    return jsonify({
        'success': True,
        'data': data,
        'count': len(data)
    })

@app.route('/api/statistics')
def get_statistics():
    """API endpoint to get dashboard statistics"""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date') or start_date
    store_id = request.args.get('store_id', 'all')
    
    stats = api.get_statistics(start_date, end_date, store_id)
    
    return jsonify({
        'success': True,
        'statistics': stats
    })

@app.route('/api/chart-data')
def get_chart_data():
    """API endpoint to get aggregated chart data"""
    try:
        view_type = request.args.get('view_type', 'hourly')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date') or start_date
        store_id = request.args.get('store_id', 'all')
        
        data = api.get_chart_data(view_type, start_date, end_date, store_id)
        
        return jsonify({
            'success': True,
            'data': data,
            'view_type': view_type,
            'count': len(data)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'view_type': view_type
        }), 500

@app.route('/api/add-data', methods=['POST'])
def add_data():
    """API endpoint to add new surveillance data (e.g., from MQTT listener)"""
    try:
        data = request.json
        # Basic validation
        required_keys = ['gerai_id', 'timestamp', 'kamera_1', 'kamera_2', 'total_customer']
        if not all(key in data for key in required_keys):
            raise ValueError("Missing required keys in data payload")

        api.insert_surveillance_data(data)
        
        return jsonify({
            'success': True,
            'message': 'Data added successfully'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 400

if __name__ == '__main__':
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
