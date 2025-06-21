import asyncio
import logging
import os
from collections import defaultdict, deque
from msg.protocol import parse_message, encode_message, make_ack, make_error, generate_message_id
from msg.monitoring import log_queue_status
from msg.utils import setup_logging
from msg.persistence import save_messages, load_messages, DATA_DIR

class SimpleMessageBroker:
    """
    SimpleMessageBroker is an asynchronous message broker supporting pub/sub and point-to-point (queue) messaging patterns.
    
    Features:
        - Asynchronous TCP server using asyncio
        - Pub/Sub and point-to-point (queue) messaging
        - In-memory and file-based persistence for reliability
        - JSON-based protocol for client communication
        - Message acknowledgment and error reporting
        - Monitoring and logging utilities
    
    Usage:
        broker = SimpleMessageBroker(host='localhost', port=8888)
        asyncio.run(broker.start())
    
    Protocol:
        - Publish: {"action": "publish", "topic": str, "message": str, "mode": "pubsub"|"queue"}
        - Subscribe: {"action": "subscribe", "topic": str, "mode": "pubsub"|"queue"}
        - Ack: {"action": "ack", "message_id": str}
    """
    def __init__(self, host='localhost', port=8888):
        """
        Initialize the broker.
        
        Args:
            host (str): Host to bind the server to.
            port (int): Port to listen on.
        """
        self.host = host
        self.port = port
        self.queues = defaultdict(asyncio.Queue)  # For pub/sub topics
        self.ptp_queues = defaultdict(deque)      # For point-to-point queues
        self.subscribers = defaultdict(list)
        self.ptp_consumers = defaultdict(list)    # For point-to-point consumers
        self.server = None
        setup_logging()
        self._load_persistent_queues()

    def _load_persistent_queues(self):
        """
        Load persisted messages from disk for all topics and queues.
        """
        for topic in os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else []:
            if topic.startswith('pubsub_'):
                t = topic[len('pubsub_'):-5]  # remove prefix and .json
                msgs = load_messages(t, 'pubsub')
                for msg, msgid in msgs:
                    self.queues[t].put_nowait((msg, msgid))
            elif topic.startswith('queue_'):
                t = topic[len('queue_'):-5]
                msgs = load_messages(t, 'queue')
                self.ptp_queues[t].extend(msgs)

    async def handle_client(self, reader, writer):
        """
        Handle an incoming client connection (producer or consumer).
        
        Args:
            reader (asyncio.StreamReader): Stream reader for the client.
            writer (asyncio.StreamWriter): Stream writer for the client.
        """
        addr = writer.get_extra_info('peername')
        logging.info(f"New connection from {addr}")
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                try:
                    message = parse_message(data)
                except Exception as e:
                    logging.error(f"Protocol error: {e}")
                    writer.write(encode_message(make_error(str(e))))
                    await writer.drain()
                    continue
                action = message.get('action')
                message_id = message.get('message_id') or generate_message_id()
                if action == 'publish':
                    topic = message.get('topic')
                    msg = message.get('message')
                    mode = message.get('mode', 'pubsub')  # 'pubsub' or 'queue'
                    logging.info(f"Received message for topic {topic}: {msg} (mode={mode})")
                    if mode == 'pubsub':
                        await self.queues[topic].put((msg, message_id))
                        await self.notify_subscribers(topic, msg, message_id)
                        # Save pubsub messages
                        msgs = list(self.queues[topic]._queue)
                        save_messages(topic, msgs, 'pubsub')
                    elif mode == 'queue':
                        self.ptp_queues[topic].append((msg, message_id))
                        await self.notify_ptp_consumer(topic)
                        # Save ptp queue
                        save_messages(topic, list(self.ptp_queues[topic]), 'queue')
                    writer.write(encode_message(make_ack(message_id)))
                    await writer.drain()
                elif action == 'subscribe':
                    topic = message.get('topic')
                    mode = message.get('mode', 'pubsub')
                    logging.info(f"Client {addr} subscribed to {topic} (mode={mode})")
                    if mode == 'pubsub':
                        self.subscribers[topic].append(writer)
                        # Deliver any undelivered messages in the queue to the new subscriber
                        for msg, msgid in list(self.queues[topic]._queue):
                            try:
                                writer.write(encode_message({'type': 'message', 'topic': topic, 'message': msg, 'message_id': msgid}))
                                await writer.drain()
                            except Exception as e:
                                logging.error(f"Error sending queued message to new subscriber: {e}")
                                break
                        while writer in self.subscribers[topic]:
                            await asyncio.sleep(0.1)
                    elif mode == 'queue':
                        self.ptp_consumers[topic].append(writer)
                        await self.notify_ptp_consumer(topic)
                        while writer in self.ptp_consumers[topic]:
                            await asyncio.sleep(0.1)
                elif action == 'ack':
                    # For future: handle message acknowledgment for persistence
                    logging.info(f"Received ACK from {addr} for message {message.get('message_id')}")
                else:
                    writer.write(encode_message(make_error(f"Unknown action: {action}", message_id)))
                    await writer.drain()
        except Exception as e:
            logging.error(f"Error handling client {addr}: {e}")
        finally:
            for topic in self.subscribers:
                if writer in self.subscribers[topic]:
                    self.subscribers[topic].remove(writer)
            for topic in self.ptp_consumers:
                if writer in self.ptp_consumers[topic]:
                    self.ptp_consumers[topic].remove(writer)
            writer.close()
            await writer.wait_closed()
            logging.info(f"Connection closed for {addr}")

    async def notify_subscribers(self, topic, message, message_id):
        """
        Send a message to all subscribers of a pub/sub topic.
        
        Args:
            topic (str): The topic name.
            message (str): The message content.
            message_id (str): Unique message identifier.
        """
        for subscriber in list(self.subscribers[topic]):
            try:
                subscriber.write(encode_message({'type': 'message', 'topic': topic, 'message': message, 'message_id': message_id}))
                await subscriber.drain()
            except Exception as e:
                logging.error(f"Error notifying subscriber: {e}")
                self.subscribers[topic].remove(subscriber)

    async def notify_ptp_consumer(self, topic):
        """
        Deliver one message to one point-to-point (queue) consumer.
        
        Args:
            topic (str): The queue name.
        """
        if self.ptp_queues[topic] and self.ptp_consumers[topic]:
            msg, message_id = self.ptp_queues[topic].popleft()
            consumer = self.ptp_consumers[topic][0]
            try:
                consumer.write(encode_message({'type': 'message', 'topic': topic, 'message': msg, 'message_id': message_id}))
                await consumer.drain()
                # Save ptp queue after delivery
                save_messages(topic, list(self.ptp_queues[topic]), 'queue')
            except Exception as e:
                logging.error(f"Error delivering to ptp consumer: {e}")
                self.ptp_consumers[topic].remove(consumer)

    async def start(self):
        """
        Start the broker server and begin accepting client connections.
        """
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logging.info(f"Broker started on {self.host}:{self.port}")
        async with self.server:
            await self.server.serve_forever()

    async def get_queue_status(self):
        """
        Return the current state of all queues for monitoring.
        
        Returns:
            dict: Dictionary with pubsub and queue sizes per topic.
        """
        status = {topic: self.queues[topic].qsize() for topic in self.queues}
        ptp_status = {topic: len(self.ptp_queues[topic]) for topic in self.ptp_queues}
        log_queue_status({'pubsub': status, 'queue': ptp_status})
        return {'pubsub': status, 'queue': ptp_status}
