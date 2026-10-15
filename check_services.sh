#!/bin/bash
echo "=== Surveillance Dashboard Health Check ==="
echo "Date: $(date)"
echo

echo "--- Service Status ---"
systemctl is-active surveillance-app.service
systemctl is-active surveillance-mqtt.service
systemctl is-active nginx
systemctl is-active postgresql

echo
echo "--- Database Connection Test ---"
cd /opt/surveillance-dashboard
source venv/bin/activate
python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='localhost',
        dbname='surveillance_db', 
        user='surveillance_user',
        password='your_secure_password_here'
    )
    print('Database: OK')
    conn.close()
except Exception as e:
    print(f'Database Error: {e}')
"

echo
echo "--- Recent Logs ---"
tail -5 /opt/surveillance-dashboard/mqtt_listener.log
