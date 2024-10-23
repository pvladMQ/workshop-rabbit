import paho.mqtt.client as mqtt
import time

# MQTT Broker details
broker = "localhost"  # Local RabbitMQ broker with MQTT plugin
port = 1883  # Default MQTT port for non-encrypted connections
topic = "test/topic"
username = "vlad"
password = "vlad"

# Create a new MQTT client instance
client = mqtt.Client(client_id="MQTTProducer", protocol=mqtt.MQTTv311)

# Set username and password for authentication
client.username_pw_set(username, password)

# Connect to the MQTT broker
client.connect(broker, port, keepalive=60)

# Start the loop
client.loop_start()  # Non-blocking loop to handle network traffic

try:
    # Send messages continuously every second
    message_count = 1
    while True:
        message = f"Message {message_count} sent to topic {topic}"
        client.publish(topic, message)
        print(f"Message '{message}' sent to topic '{topic}'")
        message_count += 1
        time.sleep(1)  # Wait for 1 second before sending the next message
except KeyboardInterrupt:
    # Disconnect gracefully when the script is interrupted
    print("Disconnecting from broker...")
    client.disconnect()
