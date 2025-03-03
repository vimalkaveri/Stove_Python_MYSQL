import time
import paho.mqtt.client as mqtt
import pandas as pd
import os

# Define MQTT Broker details
BROKER = "194.195.86.201"  # Replace with your MQTT broker address
PORT = 8883  # Typically 1883 for non-TLS, 8883 for TLS
USERNAME = "sife"  # Replace with your MQTT username
PASSWORD = "IizX9jBoVBLARO1"  # Replace with your MQTT password
TOPIC = "fe-860657056614981/data"

# Function to get the Excel file path based on the current date
def get_excel_file_path():
    date_str = time.strftime('%d%m%Y')
    folder_path = "E:\\python\\report"
    os.makedirs(folder_path, exist_ok=True)
    return os.path.join(folder_path, f"{date_str}.xlsx")

# Function to save data to Excel
def save_to_excel(timestamp, message):
    excel_file = get_excel_file_path()
    new_data = pd.DataFrame([[timestamp, message]], columns=["Timestamp", "Message"])

    if os.path.exists(excel_file):
        existing_data = pd.read_excel(excel_file)
        updated_data = pd.concat([existing_data, new_data], ignore_index=True)
    else:
        updated_data = new_data

    updated_data.to_excel(excel_file, index=False)
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
