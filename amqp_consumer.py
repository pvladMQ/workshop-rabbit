import pika
import time

# RabbitMQ connection details
rabbitmq_host = 'localhost'
queue_name = 'mqtt_queue'
routing_key = 'test.topic'  # This corresponds to the MQTT topic 'test/topic'
exchange_name = 'amq.topic'  # Default topic exchange for MQTT messages in RabbitMQ

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()

# Declare the queue (make it durable)
channel.queue_declare(queue=queue_name, durable=True)

# Bind the queue to the 'amq.topic' exchange with the routing key 'test.topic'
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

print(f"Queue '{queue_name}' bound to exchange '{exchange_name}' with routing key '{routing_key}'")

# Callback function to process messages
def callback(ch, method, properties, body):
    print(f"Received message: {body.decode()}")
    time.sleep(5)  # Simulate slow processing by delaying for 5 seconds
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Set up the consumer
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(f"Waiting for messages in queue '{queue_name}'. To exit press CTRL+C")
channel.start_consuming()
