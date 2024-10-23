import asyncio
import signal

from rstream import (
    AMQPMessage,
    Consumer,
    ConsumerOffsetSpecification,
    MessageContext,
    OffsetType,
    amqp_decoder,
)

STREAM = "my-test-stream"
START_OFFSET = 10  # Offset to start consuming messages
MESSAGE_LIMIT = 50  # Limit the number of messages to consume

async def consume():
    consumer = Consumer(
        host="localhost",
        port=5552,
        vhost="/",
        username="vlad",
        password="vlad",
    )

    consumed_count = 0  # Track the number of messages consumed

    # Handle message consumption
    async def on_message(msg: AMQPMessage, message_context: MessageContext):
        nonlocal consumed_count

        # Increment consumed count and process the message
        consumed_count += 1
        stream = message_context.consumer.get_stream(message_context.subscriber_name)
        offset = message_context.offset

        print(f"Got message: {msg.body} from stream {stream}, offset {offset}")

        # Stop after reaching the message limit
        if consumed_count >= MESSAGE_LIMIT:
            print(f"Consumed {MESSAGE_LIMIT} messages, stopping consumer.")
            await consumer.close()

    await consumer.start()

    # Use OffsetType.OFFSET to start from a specific offset
    await consumer.subscribe(
        stream=STREAM,
        callback=on_message,
        decoder=amqp_decoder,
        offset_specification=ConsumerOffsetSpecification(OffsetType.OFFSET, START_OFFSET),
    )

    try:
        await consumer.run()
    except KeyboardInterrupt:
        print("Shutting down consumer...")
        await consumer.close()

asyncio.run(consume())
