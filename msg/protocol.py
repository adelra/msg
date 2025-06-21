"""
Protocol handling for msg message broker.

Defines the JSON-based protocol for client-broker communication, including message parsing, encoding, and helpers for ACK and error messages.

Protocol message types:
    - Publish: {"action": "publish", "topic": str, "message": str, "mode": "pubsub"|"queue", "message_id": str}
    - Subscribe: {"action": "subscribe", "topic": str, "mode": "pubsub"|"queue"}
    - Ack: {"type": "ack", "message_id": str}
    - Error: {"type": "error", "error": str, "message_id": str}
    - Delivered message: {"type": "message", "topic": str, "message": str, "message_id": str}
"""
import json
import uuid

class ProtocolError(Exception):
    """
    Exception raised for protocol parsing or encoding errors.
    """
    pass

def parse_message(data: bytes) -> dict:
    """
    Parse a JSON-encoded message from bytes.
    
    Args:
        data (bytes): The received data.
    Returns:
        dict: The decoded message.
    Raises:
        ProtocolError: If the message is not valid JSON.
    """
    try:
        return json.loads(data.decode())
    except Exception as e:
        raise ProtocolError(f"Invalid message format: {e}")

def encode_message(message: dict) -> bytes:
    """
    Encode a message dictionary as JSON bytes.
    
    Args:
        message (dict): The message to encode.
    Returns:
        bytes: The JSON-encoded message.
    """
    return json.dumps(message).encode()

def make_ack(message_id: str) -> dict:
    """
    Create an ACK message for a given message ID.
    
    Args:
        message_id (str): The message ID to acknowledge.
    Returns:
        dict: The ACK message.
    """
    return {"type": "ack", "message_id": message_id}

def make_error(error: str, message_id: str = None) -> dict:
    """
    Create an error message.
    
    Args:
        error (str): Error description.
        message_id (str, optional): Related message ID.
    Returns:
        dict: The error message.
    """
    msg = {"type": "error", "error": error}
    if message_id:
        msg["message_id"] = message_id
    return msg

def generate_message_id() -> str:
    """
    Generate a unique message ID (UUID4).
    Returns:
        str: A new unique message ID.
    """
    return str(uuid.uuid4())
