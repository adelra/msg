"""
Utility functions for msg broker.

Provides logging setup and shared utilities for the broker.
"""
import logging

def setup_logging():
    """
    Configure logging for the msg broker.
    Sets log level to INFO and a standard format with timestamps.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
