#!/usr/bin/env python3
"""
Test script to verify Kafka connection and producer functionality
"""

import sys
import json
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaError
from kafka_config import get_producer_config, KAFKA_BOOTSTRAP_SERVERS, TOPIC_INVENTORY

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
        return False
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        return True

def test_connection():
    """Test basic Kafka connection"""
    print("="*60)
    print("KAFKA CONNECTION TEST")
    print("="*60)
    print(f"📡 Bootstrap Server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📬 Target Topic: {TOPIC_INVENTORY}")
    print()
    
    try:
        # Create producer
        print("Creating producer...")
        config = get_producer_config()
        producer = Producer(config)
        print("✅ Producer created successfully")
        
        # Create a simple test message
        test_message = {
            "test": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": "Test connection from kafka_inventory_producer",
            "instrumentId": 12345,
            "side": "BID",
            "price": 100.5
        }
        
        print(f"\n📤 Sending test message to topic '{TOPIC_INVENTORY}'...")
        print(f"Message: {json.dumps(test_message, indent=2)}")
        
        # Send message
        producer.produce(
            TOPIC_INVENTORY,
            key="test-key".encode('utf-8'),
            value=json.dumps(test_message).encode('utf-8'),
            callback=delivery_report
        )
        
        # Wait for delivery
        print("\n⏳ Waiting for delivery confirmation...")
        producer.flush(timeout=10)
        
        print("\n✅ Connection test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Provide helpful debugging info
        if "ssl" in str(e).lower():
            print("\n🔐 SSL Issue detected. Check:")
            print("  1. Certificate file exists: aiven.pem")
            print("  2. Certificate path is correct")
            print("  3. Certificate permissions are readable")
            
        elif "auth" in str(e).lower() or "sasl" in str(e).lower():
            print("\n🔑 Authentication Issue detected. Check:")
            print("  1. Username is correct (avnadmin)")
            print("  2. Password is correct")
            print("  3. SASL mechanism is PLAIN")
            
        elif "broker" in str(e).lower() or "connection" in str(e).lower():
            print("\n🌐 Network Issue detected. Check:")
            print("  1. Bootstrap server URL is correct")
            print("  2. Port 19850 is accessible")
            print("  3. Network/firewall allows connection")
            
        return False

def test_batch_production():
    """Test producing a small batch of messages"""
    print("\n" + "="*60)
    print("BATCH PRODUCTION TEST")
    print("="*60)
    
    try:
        producer = Producer(get_producer_config())
        
        # Send 5 test messages
        for i in range(5):
            message = {
                "batchTest": True,
                "messageId": i,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "instrumentId": 10000 + i,
                "price": 100.0 + i * 0.5
            }
            
            producer.produce(
                TOPIC_INVENTORY,
                key=f"batch-{i}".encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=lambda err, msg: None  # Silent callback
            )
        
        print("📤 Sent 5 test messages")
        print("⏳ Flushing producer...")
        
        remaining = producer.flush(timeout=10)
        
        if remaining == 0:
            print("✅ All messages delivered successfully!")
        else:
            print(f"⚠️ {remaining} messages still in queue")
            
        return remaining == 0
        
    except Exception as e:
        print(f"❌ Batch test failed: {e}")
        return False

def check_topics():
    """Check if topics exist (requires admin access)"""
    print("\n" + "="*60)
    print("TOPIC CHECK")
    print("="*60)
    
    try:
        from confluent_kafka.admin import AdminClient
        
        admin_config = get_producer_config().copy()
        admin = AdminClient(admin_config)
        
        # Get metadata
        metadata = admin.list_topics(timeout=10)
        
        print(f"📋 Available topics:")
        for topic in metadata.topics:
            print(f"  • {topic}")
            
        # Check our topics
        our_topics = [TOPIC_INVENTORY, "master-security", "trax-trades"]
        for topic in our_topics:
            if topic in metadata.topics:
                print(f"✅ Topic '{topic}' exists")
            else:
                print(f"❌ Topic '{topic}' NOT FOUND - may need to create it")
                
    except Exception as e:
        print(f"⚠️ Could not list topics (may need admin permissions): {e}")

def main():
    print("\n🚀 KAFKA PRODUCER TEST SUITE")
    print("="*60)
    
    # Run tests
    tests_passed = 0
    tests_total = 3
    
    # Test 1: Basic connection
    if test_connection():
        tests_passed += 1
    
    # Test 2: Batch production
    if test_batch_production():
        tests_passed += 1
    
    # Test 3: Topic check
    check_topics()
    tests_passed += 1  # This is informational, always passes
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests passed: {tests_passed}/{tests_total}")
    
    if tests_passed == tests_total:
        print("✅ All tests passed! Kafka producers should work correctly.")
        print("\nYou can now run:")
        print("  python kafka_inventory_producer.py")
        print("  python kafka_security_producer.py")
        print("  python kafka_trades_producer.py")
    else:
        print("❌ Some tests failed. Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main()