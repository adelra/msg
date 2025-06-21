"""
Monitoring utilities for msg broker.

Provides logging and monitoring helpers for queue status and broker health.
"""
import logging

def log_queue_status(status: dict):
    """
    Log the current status of all queues.
    
    Args:
        status (dict): Dictionary with queue/topic sizes.
    """
    logging.info(f"Queue status: {status}")
