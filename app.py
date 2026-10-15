from flask import Flask, render_template, jsonify, request
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import threading
import time
import logging
import os
from dotenv import load_dotenv

app = Flask(__name__)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "host": os.getenv('DB_HOST', 'localhost'),
    "dbname": os.getenv('DB_NAME', 'surveillance_data'),
    "user": os.getenv('DB_USER', 'admin'),
    "password": os.getenv('DB_PASSWORD', 'Admin123')
}

class SurveillanceAPI:
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        """Initialize the PostgreSQL database"""
        try:
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
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_gerai_id ON surveillance_data(gerai_id)')
            
            conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
        finally:
            if conn:
                conn.close()
    
    def get_connection(self):
        """Get database connection with error handling"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def get_available_stores(self):
        """Get list of all stores with recent data"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
                SELECT 
                    gerai_id,
                    COUNT(*) as record_count,
                    MIN(timestamp) as first_record,
                    MAX(timestamp) as last_record,
                    SUM(total_customer) as total_customers
                FROM surveillance_data 
                GROUP BY gerai_id 
                ORDER BY MAX(timestamp) DESC
            """
            
            cursor.execute(query)
            stores = cursor.fetchall()
            
            return [dict(store) for store in stores]
            
        except Exception as e:
            logger.error(f"Error getting available stores: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_surveillance_data(self, start_date=None, end_date=None, store_id=None, limit=1000):
        """Get surveillance data with optional filters"""
        try:
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
            
            # Convert to list of dictionaries with proper datetime handling
            result = []
            for row in rows:
                data = dict(row)
                # Convert timestamps to ISO format for JSON serialization
                if 'timestamp' in data and data['timestamp']:
                    data['timestamp'] = data['timestamp'].isoformat()
                if 'created_at' in data and data['created_at']:
                    data['created_at'] = data['created_at'].isoformat()
                result.append(data)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting surveillance data: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    def get_statistics(self, start_date=None, end_date=None, store_id=None):
        """Get dashboard statistics with better error handling"""
        try:
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
            cursor.execute(f"SELECT COALESCE(SUM(total_customer), 0) as total FROM surveillance_data {conditions}", params)
            total_customers = cursor.fetchone()['total'] or 0
            
            # Active stores
            cursor.execute(f"SELECT COUNT(DISTINCT gerai_id) as count FROM surveillance_data {conditions}", params)
            active_stores = cursor.fetchone()['count'] or 0
            
            # Average customers per hour
            cursor.execute(f"SELECT COALESCE(AVG(total_customer), 0) as avg FROM surveillance_data {conditions}", params)
            avg_customers = cursor.fetchone()['avg'] or 0
            
            # Peak hour (most busy hour) with better handling
            peak_hour_query = f"""
                SELECT EXTRACT(HOUR FROM timestamp) as hour, SUM(total_customer) as total
                FROM surveillance_data {conditions}
                GROUP BY EXTRACT(HOUR FROM timestamp)
                ORDER BY total DESC
                LIMIT 1
            """
            cursor.execute(peak_hour_query, params)
            peak_hour_result = cursor.fetchone()
            peak_hour = f"{int(peak_hour_result['hour']):02d}:00" if peak_hour_result and peak_hour_result['hour'] is not None else '--:--'
            
            return {
                'total_customers': int(total_customers),
                'active_stores': int(active_stores),
                'avg_customers': round(float(avg_customers), 1),
                'peak_hour': peak_hour
            }
            
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {
                'total_customers': 0,
                'active_stores': 0,
                'avg_customers': 0.0,
                'peak_hour': '--:--'
            }
        finally:
            if conn:
                conn.close()
    
    def get_chart_data(self, view_type, start_date=None, end_date=None, store_id=None):
        """Get aggregated data for charts with improved error handling"""
        try:
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

            # Enhanced queries for each view type
            if view_type == 'hourly':
                query = """
                    SELECT 
                        DATE_TRUNC('hour', timestamp) as hour,
                        gerai_id,
                        COALESCE(SUM(total_customer), 0) as total_customers,
                        COALESCE(SUM(kamera_1), 0) as total_kamera_1,
                        COALESCE(SUM(kamera_2), 0) as total_kamera_2,
                        COUNT(*) as record_count
                    FROM surveillance_data
                    {conditions}
                    GROUP BY DATE_TRUNC('hour', timestamp), gerai_id
                    ORDER BY hour ASC, gerai_id ASC
                """.format(conditions=conditions)
            elif view_type == 'daily':
                query = """
                    SELECT 
                        DATE_TRUNC('day', timestamp) as day,
                        gerai_id,
                        COALESCE(SUM(total_customer), 0) as total_customers,
                        COALESCE(SUM(kamera_1), 0) as total_kamera_1,
                        COALESCE(SUM(kamera_2), 0) as total_kamera_2,
                        COUNT(*) as record_count
                    FROM surveillance_data
                    {conditions}
                    GROUP BY DATE_TRUNC('day', timestamp), gerai_id
                    ORDER BY day ASC, gerai_id ASC
                """.format(conditions=conditions)
            elif view_type == 'weekly':
                query = """
                    SELECT 
                        DATE_TRUNC('week', timestamp) as week,
                        gerai_id,
                        COALESCE(SUM(total_customer), 0) as total_customers,
                        COALESCE(SUM(kamera_1), 0) as total_kamera_1,
                        COALESCE(SUM(kamera_2), 0) as total_kamera_2,
                        COUNT(*) as record_count
                    FROM surveillance_data
                    {conditions}
                    GROUP BY DATE_TRUNC('week', timestamp), gerai_id
                    ORDER BY week ASC, gerai_id ASC
                """.format(conditions=conditions)
            else:
                logger.warning(f"Invalid view_type: {view_type}")
                return []

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
                
            logger.info(f"Chart data query returned {len(result)} records for view_type: {view_type}")
            return result
            
        except Exception as e:
            logger.error(f"Error in get_chart_data: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def insert_surveillance_data(self, data):
        """Insert new surveillance data with validation"""
        try:
            # Validate required fields
            required_fields = ['gerai_id', 'timestamp', 'kamera_1', 'kamera_2', 'total_customer']
            for field in required_fields:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate data types
            data['kamera_1'] = int(data['kamera_1'])
            data['kamera_2'] = int(data['kamera_2'])
            data['total_customer'] = int(data['total_customer'])
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO surveillance_data (gerai_id, timestamp, kamera_1, kamera_2, total_customer)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                data['gerai_id'],
                data['timestamp'],
                data['kamera_1'],
                data['kamera_2'],
                data['total_customer']
            ))
            
            inserted_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"Data inserted successfully with ID: {inserted_id}")
            return inserted_id
            
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def get_data_summary(self):
        """Get summary of data in database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT gerai_id) as unique_stores,
                    MIN(timestamp) as earliest_record,
                    MAX(timestamp) as latest_record,
                    SUM(total_customer) as total_customers_all_time
                FROM surveillance_data
            """
            
            cursor.execute(query)
            summary = cursor.fetchone()
            
            if summary:
                result = dict(summary)
                # Format timestamps
                if result['earliest_record']:
                    result['earliest_record'] = result['earliest_record'].isoformat()
                if result['latest_record']:
                    result['latest_record'] = result['latest_record'].isoformat()
                return result
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting data summary: {e}")
            return {}
        finally:
            if conn:
                conn.close()

# Initialize API
api = SurveillanceAPI()

# --- Flask Routes ---

@app.route('/')
def dashboard():
    """Serve the dashboard HTML"""
    return render_template('dashboard.html')

@app.route('/api/stores')
def get_stores():
    """API endpoint to get all available stores"""
    try:
        stores = api.get_available_stores()
        return jsonify({
            'success': True,
            'stores': stores,
            'count': len(stores)
        })
    except Exception as e:
        logger.error(f"Error in get_stores: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/data')
def get_data():
    """API endpoint to get raw surveillance data with filters"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date') or start_date 
        store_id = request.args.get('store_id', 'all')
        limit = request.args.get('limit', 1000, type=int)
        
        data = api.get_surveillance_data(start_date, end_date, store_id, limit)
        
        return jsonify({
            'success': True,
            'data': data,
            'count': len(data),
            'filters': {
                'start_date': start_date,
                'end_date': end_date,
                'store_id': store_id,
                'limit': limit
            }
        })
    except Exception as e:
        logger.error(f"Error in get_data: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/statistics')
def get_statistics():
    """API endpoint to get dashboard statistics"""
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date') or start_date
        store_id = request.args.get('store_id', 'all')
        
        stats = api.get_statistics(start_date, end_date, store_id)
        
        return jsonify({
            'success': True,
            'statistics': stats,
            'filters': {
                'start_date': start_date,
                'end_date': end_date,
                'store_id': store_id
            }
        })
    except Exception as e:
        logger.error(f"Error in get_statistics: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/chart-data')
def get_chart_data():
    """API endpoint to get aggregated chart data"""
    try:
        view_type = request.args.get('view_type', 'hourly')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date') or start_date
        store_id = request.args.get('store_id', 'all')
        
        # Validate view_type
        if view_type not in ['hourly', 'daily', 'weekly']:
            raise ValueError(f"Invalid view_type: {view_type}. Must be one of: hourly, daily, weekly")
        
        data = api.get_chart_data(view_type, start_date, end_date, store_id)
        
        return jsonify({
            'success': True,
            'data': data,
            'view_type': view_type,
            'count': len(data),
            'filters': {
                'start_date': start_date,
                'end_date': end_date,
                'store_id': store_id
            }
        })
    except Exception as e:
        logger.error(f"Error in get_chart_data: {e}")
        return jsonify({
            'success': False,
            'message': str(e),
            'view_type': view_type
        }), 500

@app.route('/api/summary')
def get_summary():
    """API endpoint to get database summary"""
    try:
        summary = api.get_data_summary()
        return jsonify({
            'success': True,
            'summary': summary
        })
    except Exception as e:
        logger.error(f"Error in get_summary: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/add-data', methods=['POST'])
def add_data():
    """API endpoint to add new surveillance data"""
    try:
        data = request.json
        if not data:
            raise ValueError("No JSON data provided")

        inserted_id = api.insert_surveillance_data(data)
        
        return jsonify({
            'success': True,
            'message': 'Data added successfully',
            'inserted_id': inserted_id,
            'data': data
        })
    except ValueError as ve:
        logger.warning(f"Validation error in add_data: {ve}")
        return jsonify({
            'success': False,
            'message': str(ve)
        }), 400
    except Exception as e:
        logger.error(f"Error in add_data: {e}")
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        summary = api.get_data_summary()
        return jsonify({
            'success': True,
            'status': 'healthy',
            'database': 'connected',
            'summary': summary,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'message': 'Endpoint not found'
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'message': 'Internal server error'
    }), 500

if __name__ == '__main__':
    logger.info("Starting Surveillance Dashboard API...")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['dbname']}")
    
    # Test database connection on startup
    try:
        summary = api.get_data_summary()
        logger.info(f"Database connected successfully. Summary: {summary}")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
