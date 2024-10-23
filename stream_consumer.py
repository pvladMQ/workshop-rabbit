import asyncio
import time
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    MessageContext,
    amqp_decoder,
)

STREAM = "my-test-stream"
MESSAGES = 1_000_000  # Assuming we're consuming 1,000,000 messages

async def consume():
    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="vlad",
        password="vlad",
    )

    start_time = time.perf_counter()  # Start the timer
    consumed_count = 0

    # Handle shutdown gracefully using try...except for Windows compatibility
    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        nonlocal consumed_count
        nonlocal start_time

        consumed_count += 1
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset

        # Check if we have consumed the expected number of messages
        if consumed_count == MESSAGES:
            end_time = time.perf_counter()  # End the timer
            print(f"Consumed {MESSAGES} messages in {end_time - start_time:0.4f} seconds")
            await consumer.close()

    await consumer.start()
    await consumer.subscribe(stream=STREAM, callback=on_message, decoder=amqp_decoder)

    try:
        await consumer.run()
    except KeyboardInterrupt:
        print("Shutting down consumer...")
        await consumer.close()

asyncio.run(consume())
