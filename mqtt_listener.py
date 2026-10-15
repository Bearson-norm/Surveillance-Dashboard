import paho.mqtt.client as mqtt
import json
import psycopg2
from datetime import datetime
import logging
import time
import os
from dotenv import load_dotenv


load_dotenv()

DB_CONFIG = {
    "host": os.getenv('DB_HOST', 'localhost'),
    "dbname": os.getenv('DB_NAME', 'surveillance_store_db'),
    "user": os.getenv('DB_USER', 'admin'),
    "password": os.getenv('DB_PASSWORD', 'Admin123')
}

# Configure logging for Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mqtt_listener.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


# MQTT configuration
MQTT_BROKER = "103.87.67.139"
MQTT_PORT = 1883
MQTT_TOPIC = "gerai/customer_count"

def get_database_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        return None

def test_database_connection():
    """Test database connectivity"""
    try:
        conn = get_database_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logging.info(f"Database connected successfully: {version[0]}")
            cursor.close()
            conn.close()
            return True
        return False
    except Exception as e:
        logging.error(f"Database connection test failed: {e}")
        return False

def init_database():
    """Initialize the database and create required tables"""
    try:
        conn = get_database_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Create surveillance_data table (matching dashboard expectation)
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
        
        # Also create the old table name for backward compatibility
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS surveilance_data_dummy (
                id SERIAL PRIMARY KEY,
                gerai_id VARCHAR(255) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                kamera_1 INTEGER NOT NULL,
                kamera_2 INTEGER NOT NULL,
                total_customer INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better query performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_gerai_timestamp ON surveillance_data(gerai_id, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON surveillance_data(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_gerai_timestamp_dummy ON surveilance_data_dummy(gerai_id, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp_dummy ON surveilance_data_dummy(timestamp)')
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logging.error(f"Database initialization error: {e}")
        return False

def save_surveillance_data(payload):
    """Save received surveillance data to both tables"""
    try:
        conn = get_database_connection()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        # Insert into main table (surveillance_data)
        cursor.execute('''
            INSERT INTO surveillance_data 
                (gerai_id, timestamp, kamera_1, kamera_2, total_customer)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            payload['gerai_id'],
            payload['timestamp'],
            payload['kamera_1'],
            payload['kamera_2'],
            payload['total_customer']
        ))
        
        inserted_id = cursor.fetchone()[0]
        
        # Also insert into backup table for compatibility
        cursor.execute('''
            INSERT INTO surveilance_data_dummy 
                (gerai_id, timestamp, kamera_1, kamera_2, total_customer)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            payload['gerai_id'],
            payload['timestamp'],
            payload['kamera_1'],
            payload['kamera_2'],
            payload['total_customer']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Data saved successfully with ID: {inserted_id} - Store: {payload['gerai_id']}, Total: {payload['total_customer']}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        return False

def validate_payload(payload):
    """Validate the received payload"""
    required_fields = ['gerai_id', 'timestamp', 'kamera_1', 'kamera_2', 'total_customer']
    
    for field in required_fields:
        if field not in payload:
            logging.error(f"Missing required field: {field}")
            return False
    
    # Validate data types
    try:
        payload['kamera_1'] = int(payload['kamera_1'])
        payload['kamera_2'] = int(payload['kamera_2'])
        payload['total_customer'] = int(payload['total_customer'])
    except (ValueError, TypeError) as e:
        logging.error(f"Invalid data types in payload: {e}")
        return False
    
    # Validate timestamp format
    try:
        datetime.fromisoformat(payload['timestamp'].replace('T', ' '))
    except ValueError as e:
        logging.error(f"Invalid timestamp format: {e}")
        return False
    
    return True

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    if rc == 0:
        logging.info(f"Connected to MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        logging.info(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        logging.error(f"Failed to connect to MQTT broker with code: {rc}")
        error_messages = {
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        if rc in error_messages:
            logging.error(f"MQTT Error: {error_messages[rc]}")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected from MQTT broker"""
    if rc != 0:
        logging.warning(f"Unexpected MQTT disconnection with code: {rc}")
    else:
        logging.info("MQTT disconnected cleanly")

def on_message(client, userdata, msg):
    """Callback when message is received"""
    try:
        payload_str = msg.payload.decode()
        logging.info(f"Raw message received: {payload_str}")
        
        payload = json.loads(payload_str)
        logging.info(f"Parsed payload: Store={payload.get('gerai_id', 'Unknown')}, Total={payload.get('total_customer', 0)}")
        
        if not validate_payload(payload):
            logging.error("Payload validation failed")
            return
        
        if save_surveillance_data(payload):
            logging.info("Data processed and saved successfully")
        else:
            logging.error("Failed to save data to database")
            
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON payload: {e}")
        logging.error(f"Raw payload: {msg.payload}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    """Main function to run the listener server"""
    logging.info("Starting MQTT Listener for Customer Detection System")
    
    # Test database connection
    if not test_database_connection():
        logging.error("Cannot connect to database. Please check your database configuration.")
        input("Press Enter to exit...")
        return
    
    # Initialize database
    if not init_database():
        logging.error("Failed to initialize database. Exiting...")
        input("Press Enter to exit...")
        return

    # Setup MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to MQTT broker
    try:
        logging.info(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        logging.info(f"MQTT Listener started successfully")
        logging.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['dbname']}")
        logging.info(f"MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        logging.info(f"Topic: {MQTT_TOPIC}")
        logging.info("Waiting for messages... (Press Ctrl+C to stop)")
        
        # Start the loop
        client.loop_forever()
        
    except KeyboardInterrupt:
        logging.info("Shutting down MQTT listener...")
        client.disconnect()
        logging.info("MQTT listener stopped successfully")
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()
