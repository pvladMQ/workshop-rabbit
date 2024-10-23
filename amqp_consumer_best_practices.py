import pika
import time

# RabbitMQ connection details
rabbitmq_host = 'localhost'
exchange_name = 'direct_exchange'
routing_key = 'direct_routing_key'
queue_name = 'quorum_queue'

# Set up connection parameters
params = pika.ConnectionParameters(
    host=rabbitmq_host,
    heartbeat=600,  # Keep connection alive
    blocked_connection_timeout=300  # Handle blocked connections
)

# Connect to RabbitMQ
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Start transaction mode for reliability
channel.tx_select()

# Declare the quorum queue (in case it's not already created)
channel.queue_declare(queue=queue_name, durable=True, arguments={"x-queue-type": "quorum"})

# Declare the direct exchange
channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

# Bind the queue to the exchange with the routing key
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

# Callback function for processing messages
def callback(ch, method, properties, body):
    try:
        message = body.decode()
        print(f"Received message: {message}")

        # Simulate message processing
        time.sleep(5)  # Slow down for demonstration purposes

        # Manually acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Commit transaction after message is processed
        channel.tx_commit()
        print(f"Message '{message}' processed and transaction committed.")

    except Exception as e:
        # If something goes wrong, rollback the transaction
        channel.tx_rollback()
        print(f"Error processing message: {e}. Transaction rolled back.")

# Set up consumer with manual acknowledgments
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(f"Waiting for messages in queue '{queue_name}'. To exit press CTRL+C")
channel.start_consuming()
