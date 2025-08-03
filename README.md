# msg

msg short for message but also monosodium glutamate. A simple, extensible, and easy-to-debug Python message broker. Supports pub/sub and point-to-point (queue) messaging with a minimal JSON protocol over TCP. Built for clarity, reliability, and Pythonic integration.

---

## Features
- **Asyncio-based**: Handles many clients efficiently with Python's standard library.
- **Pub/Sub & Queue**: Both publish/subscribe and point-to-point (queue) messaging patterns.
- **Persistence**: File-based message persistence for reliability.
- **Simple Protocol**: JSON over TCP, easy to debug and extend.
- **Monitoring**: Built-in logging and queue status reporting.
- **Minimal Dependencies**: Pure Python, installable with `uv` or `pip`.

---

## Architecture

```mermaid
flowchart TD
    subgraph Producer/Publisher
        A1[Producer Client]
    end
    subgraph Consumer/Subscriber
        B1[Subscriber Client]
        B2[Queue Consumer]
    end
    subgraph Broker
        C1[Asyncio TCP Server]
        C2[In-Memory Queues]
        C3[Persistence Layer]
        C4[Monitoring/Logging]
    end
    A1 -- Publish JSON Message --> C1
    B1 -- Subscribe JSON Message --> C1
    B2 -- Subscribe (Queue) --> C1
    C1 -- Deliver Message --> B1
    C1 -- Deliver Message (Queue) --> B2
    C1 -- Save/Load --> C3
    C1 -- Log/Status --> C4
```

---

## Quick Start

### Install
```bash
uv pip install .
```

### Run the Broker
```bash
python -m msg  # or python -m msg.broker (both supported)
```

### Example: Publish a Message
```python
import asyncio
import json

async def publish_message():
    reader, writer = await asyncio.open_connection('localhost', 8888)
    msg = json.dumps({"action": "publish", "topic": "news", "message": "Hello, world!"})
    writer.write(msg.encode())
    await writer.drain()
    data = await reader.read(1024)
    print("ACK:", data.decode())
    writer.close()
    await writer.wait_closed()

asyncio.run(publish_message())
```

### Example: Subscribe to a Topic
```python
import asyncio
import json

async def subscribe():
    reader, writer = await asyncio.open_connection('localhost', 8888)
    msg = json.dumps({"action": "subscribe", "topic": "news"})
    writer.write(msg.encode())
    await writer.drain()
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print("Received:", json.loads(data.decode()))
    writer.close()
    await writer.wait_closed()

# Run in another script or terminal:
# asyncio.run(subscribe())
```

---

## Protocol

All communication is via JSON messages over TCP sockets.

- **Publish:**
  ```json
  {"action": "publish", "topic": "news", "message": "Hello!", "mode": "pubsub"}
  ```
- **Subscribe:**
  ```json
  {"action": "subscribe", "topic": "news", "mode": "pubsub"}
  ```
- **Queue Publish:**
  ```json
  {"action": "publish", "topic": "jobs", "message": "do work", "mode": "queue"}
  ```
- **Queue Subscribe:**
  ```json
  {"action": "subscribe", "topic": "jobs", "mode": "queue"}
  ```
- **ACK:**
  ```json
  {"type": "ack", "message_id": "..."}
  ```
- **Delivered Message:**
  ```json
  {"type": "message", "topic": "news", "message": "Hello!", "message_id": "..."}
  ```
- **Error:**
  ```json
  {"type": "error", "error": "...", "message_id": "..."}
  ```

---

## Persistence
- Messages are saved to `.msg_data/` as JSON files per topic/queue.
- On broker restart, undelivered messages are reloaded.
- The persistence layer is implemented in `msg/persistence.py`.

---

## Testing
- Unit and end-to-end integration tests are provided in the `tests/` directory.
- Run all tests:
  ```bash
  uv pip install pytest pytest-asyncio
  pytest
  ```
- Example tests include:
  - Saving and loading pub/sub messages.
  - Saving and loading queue messages.
  - Deleting messages.

---
## Current bugs

   * Unbounded Memory Usage: The ptp_queues (point-to-point queues) are implemented using collections.deque, which can grow indefinitely if there are no consumers, leading to a potential memory exhaustion issue.
   * No Acknowledgment for PTP Queues: In notify_ptp_consumer, a message is removed from the queue as soon as it is sent to a consumer, without waiting for an acknowledgment. If the consumer crashes before processing
     the message, the message is lost.
   * Inefficient Message Delivery to New Subscribers: When a new subscriber connects to a pub/sub topic, it receives all the messages currently in the queue for that topic. This is done by iterating over a copy of the
     queue, which can be inefficient for large queues.
   * Potential Race Condition in PTP Queues: In notify_ptp_consumer, the code checks if there are messages in the queue and if there are consumers, and then proceeds to send the message. However, there is a potential
     race condition where a consumer could disconnect between the check and the send call, which would result in an error.
   * No Error Handling for `writer.drain()`: The code does not handle potential errors that can occur during writer.drain(), which can lead to unhandled exceptions and connection termination.
   * Blocking File I/O: The save_messages and load_messages functions perform blocking file I/O, which can block the entire event loop and degrade performance. These operations should be run in a separate thread or
     using an asynchronous file I/O library.

---


## Contributing
- Fork, branch, and submit pull requests.
- All code should be type-annotated and documented.
- Please add or update tests for new features.
