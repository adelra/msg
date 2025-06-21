"""
Persistence layer for msg broker.
Implements simple file-based persistence for pubsub and point-to-point queues.

Functions:
    save_messages(topic, messages, mode): Save messages to disk for a topic/queue.
    load_messages(topic, mode): Load messages from disk for a topic/queue.
    delete_messages(topic, mode): Delete persisted messages for a topic/queue.
"""
import json
import os
from typing import List, Tuple

DATA_DIR = os.environ.get("msg_DATA_DIR", ".msg_data")

os.makedirs(DATA_DIR, exist_ok=True)

def _topic_file(topic: str, mode: str) -> str:
    """
    Get the file path for a topic/queue's persisted messages.
    
    Args:
        topic (str): The topic or queue name.
        mode (str): 'pubsub' or 'queue'.
    Returns:
        str: Path to the JSON file for the topic/queue.
    """
    return os.path.join(DATA_DIR, f"{mode}_{topic}.json")

def save_messages(topic: str, messages: List[Tuple[str, str]], mode: str = 'pubsub'):
    """
    Save messages for a topic/queue to disk as a JSON file.
    
    Args:
        topic (str): The topic or queue name.
        messages (List[Tuple[str, str]]): List of (message, message_id) tuples.
        mode (str): 'pubsub' or 'queue'.
    """
    path = _topic_file(topic, mode)
    with open(path, 'w') as f:
        json.dump(messages, f)

def load_messages(topic: str, mode: str = 'pubsub') -> List[Tuple[str, str]]:
    """
    Load messages for a topic/queue from disk.
    
    Args:
        topic (str): The topic or queue name.
        mode (str): 'pubsub' or 'queue'.
    Returns:
        List[Tuple[str, str]]: List of (message, message_id) tuples.
    """
    path = _topic_file(topic, mode)
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return json.load(f)

def delete_messages(topic: str, mode: str = 'pubsub'):
    """
    Delete persisted messages for a topic/queue from disk.
    
    Args:
        topic (str): The topic or queue name.
        mode (str): 'pubsub' or 'queue'.
    """
    path = _topic_file(topic, mode)
    if os.path.exists(path):
        os.remove(path)
