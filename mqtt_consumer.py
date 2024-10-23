import paho.mqtt.client as mqtt
import time

# MQTT Broker details
broker = "localhost"  # Local RabbitMQ broker with MQTT plugin
port = 1883  # Default MQTT port for non-encrypted connections
topic = "test/topic"
username = "vlad"
password = "vlad"

# Define the callback function for when a message is received
def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()} on topic {message.topic}")
    time.sleep(5)  # Simulate slow processing by delaying for 5 seconds

# Define the callback function for connection status
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully to broker")
        # Subscribe to the topic once connected
        client.subscribe(topic)
        print(f"Subscribed to topic '{topic}'")
    else:
        print(f"Failed to connect, return code {rc}")

# Create a new MQTT client instance
client = mqtt.Client(client_id="MQTTConsumer", protocol=mqtt.MQTTv311)

# Set username and password for authentication
client.username_pw_set(username, password)

# Attach callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker, port, keepalive=60)

# Start the loop to process network traffic, including handling callbacks
client.loop_forever()
