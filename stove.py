import mysql.connector
import time
import paho.mqtt.client as mqtt
import pandas as pd
import os

# Database Configuration
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "Test@2024"
DB_NAME = "stove_data"

# Define MQTT Broker details
BROKER = "194.195.86.201"  # Replace with your MQTT broker address
PORT = 8883  # Typically 1883 for non-TLS, 8883 for TLS
USERNAME = "sife"  # Replace with your MQTT username
PASSWORD = "IizX9jBoVBLARO1"  # Replace with your MQTT password
TOPIC = "fe-860657056614981/data"

# Excel File Path
EXCEL_FILE = "data.xlsx"

# Function to connect to the database
def connect_db():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
        return None

# Function to save data to Excel
def save_to_excel(timestamp, message):
    new_data = pd.DataFrame([[timestamp, message]], columns=["Timestamp", "Message"])

    if os.path.exists(EXCEL_FILE):
        existing_data = pd.read_excel(EXCEL_FILE)
        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        updated_data = new_data

    updated_data.to_excel(EXCEL_FILE, index=False)
    print(f"[{timestamp}] Message saved to Excel: {message}")

# Callback when connected to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully!")
        print(f"Subscribed to topic: {TOPIC}")
        client.subscribe(TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

# Callback when a message is received
def on_message(client, userdata, msg):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    message = msg.payload.decode()

    # Save message to the database
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            query = "INSERT INTO stove_data (timestamp, message) VALUES (%s, %s)"
            cursor.execute(query, (timestamp, message))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"[{timestamp}] Message saved to DB: {message}")
        except mysql.connector.Error as err:
            print(f"Database error: {err}")

    # Save message to Excel
    save_to_excel(timestamp, message)

# Callback when disconnected
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with return code {rc}. Reconnecting...")
    while True:
        try:
            client.reconnect()
            print("Reconnected successfully!")
            break
        except Exception as e:
            print(f"Reconnect failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Create MQTT client
client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

# Connect to MQTT broker
client.connect(BROKER, PORT, 60)

# Keep the script running
while True:
    try:
        client.loop_start()
        time.sleep(1)  # Prevent high CPU usage
    except KeyboardInterrupt:
        print("Script stopped manually.")
        client.loop_stop()
        break
    except Exception as e:
        print(f"Error: {e}")
