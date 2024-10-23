import pika
import time

# RabbitMQ connection details
rabbitmq_host = 'localhost'
exchange_name = 'direct_exchange'
routing_key = 'direct_routing_key'
queue_name = 'quorum_queue'

# Set up connection credentials
credentials = pika.PlainCredentials('vlad', 'vlad')

# Set up connection parameters
params = pika.ConnectionParameters(
    host=rabbitmq_host,
    credentials=credentials,  # Add credentials here
    heartbeat=600,  # Keep connection alive
    blocked_connection_timeout=300  # Handle blocked connections
)

# Connect to RabbitMQ
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare a direct exchange
channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

# Declare a quorum queue (durable and replicated)
channel.queue_declare(queue=queue_name, durable=True, arguments={"x-queue-type": "quorum"})

# Bind the queue to the exchange with the specified routing key
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

# Enable Publisher Confirms
channel.confirm_delivery()

# Function to publish a message
def publish_message(message):
    try:
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2  # Make message persistent
            )
        )
        print(f"Message '{message}' sent and confirmed by broker.")
    except pika.exceptions.UnroutableError:
        print(f"Message '{message}' could not be routed.")

try:
    message_count = 1
    while True:
        message = f"Message {message_count}"
        publish_message(message)
        message_count += 1
        time.sleep(1)  # Publish a message every second
except KeyboardInterrupt:
    print("Stopping producer...")

# Close the connection gracefully
connection.close()
