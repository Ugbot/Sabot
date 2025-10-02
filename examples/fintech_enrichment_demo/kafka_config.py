#!/usr/bin/env python3
"""
Shared Kafka configuration for SASL_SSL authentication
Update the values below with your Kafka cluster details
"""

import os

# -----------------------------
# Kafka Connection Settings
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"  # Update with your Kafka broker
KAFKA_SASL_USERNAME = "your-username"
KAFKA_SASL_PASSWORD = "your-password"

# SSL Certificate Path (optional, for cloud deployments)
KAFKA_SSL_CA_LOCATION = os.path.join(os.path.dirname(__file__), "ca-cert.pem")

# Optional: If using token authentication instead of password
KAFKA_USE_TOKEN = False  # Set to True if using token authentication
KAFKA_TOKEN = "your-token-here"  # Update with your token if using token auth

# -----------------------------
# Topic Names
# -----------------------------
TOPIC_INVENTORY = "inventory-rows"
TOPIC_SECURITY = "master-security"
TOPIC_TRADES = "trax-trades"

# -----------------------------
# Producer Configuration
# -----------------------------
def get_producer_config():
    """
    Returns the Kafka producer configuration for Aiven
    """
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_SASL_USERNAME,
        'ssl.ca.location': KAFKA_SSL_CA_LOCATION,  # Add SSL certificate
        'compression.type': 'snappy',
        'batch.size': 32768,  # Increased batch size for better throughput
        'linger.ms': 10,
        'acks': 1,  # Fast mode - only wait for leader acknowledgment
        'queue.buffering.max.messages': 100000,  # Increase queue size
        'queue.buffering.max.kbytes': 1048576,  # 1GB buffer
        'compression.level': 6,  # Snappy compression level
    }
    
    # Use token or password authentication
    if KAFKA_USE_TOKEN:
        config['sasl.password'] = f'token:{KAFKA_TOKEN}'
    else:
        config['sasl.password'] = KAFKA_SASL_PASSWORD
    
    return config

# -----------------------------
# Consumer Configuration (if needed)
# -----------------------------
def get_consumer_config(group_id='default-consumer-group'):
    """
    Returns the Kafka consumer configuration for Aiven
    """
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_SASL_USERNAME,
        'ssl.ca.location': KAFKA_SSL_CA_LOCATION,  # Add SSL certificate
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    
    # Use token or password authentication
    if KAFKA_USE_TOKEN:
        config['sasl.password'] = f'token:{KAFKA_TOKEN}'
    else:
        config['sasl.password'] = KAFKA_SASL_PASSWORD
    
    return config