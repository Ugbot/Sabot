#!/usr/bin/env python3
"""
Basic Streaming Pipeline Example

This example shows how to create a simple streaming pipeline with Sabot.
It demonstrates:
- Creating a Sabot app with the CLI-compatible agent model
- Processing sensor data streams with @app.agent() decorator
- Filtering and transforming streaming data
- Real-time alerts based on conditions

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker
    sabot -A examples.core.basic_pipeline:app worker

    # Or with logging
    sabot -A examples.core.basic_pipeline:app worker --loglevel=DEBUG

    # Send test data (separate terminal)
    python -c "
    from confluent_kafka import Producer
    import json, random, time

    producer = Producer({'bootstrap.servers': 'localhost:19092'})

    for i in range(50):
        data = {
            'sensor_id': f'sensor_{random.randint(1, 5)}',
            'temperature': round(random.uniform(20.0, 35.0), 2),
            'humidity': round(random.uniform(30.0, 80.0), 2),
            'timestamp': time.time()
        }
        producer.produce('sensor-readings', value=json.dumps(data).encode())
        producer.flush()
        print(f'Sent: {data}')
        time.sleep(0.5)
    "
"""

import sabot as sb

# Create Sabot application
app = sb.App(
    'sensor-pipeline',
    broker='kafka://localhost:19092',
    value_serializer='json'
)


@app.agent('sensor-readings')
async def process_sensors(stream):
    """
    Process sensor readings and alert on high temperatures.

    Pipeline:
    1. Add Celsius and Fahrenheit fields
    2. Filter for temperatures > 25°C
    3. Classify alert level (HIGH/MEDIUM)
    4. Display alerts
    """
    print("🌡️  Sensor processing agent started")
    print("📡 Monitoring sensor readings...")
    print("🔥 Only showing readings above 25°C\n")

    async for record in stream:
        try:
            # Transform: Add temperature conversions
            record['temp_celsius'] = record['temperature']
            record['temp_fahrenheit'] = round(record['temperature'] * 9/5 + 32, 2)

            # Filter: Only hot readings (> 25°C)
            if record['temperature'] > 25.0:
                # Classify alert level
                if record['temperature'] > 30.0:
                    record['alert_level'] = 'HIGH'
                    emoji = '🔥'
                else:
                    record['alert_level'] = 'MEDIUM'
                    emoji = '⚠️ '

                # Display alert
                print(f"{emoji} ALERT [{record['alert_level']}]: "
                      f"{record['sensor_id']} - "
                      f"{record['temp_celsius']}°C "
                      f"({record['temp_fahrenheit']}°F) "
                      f"| Humidity: {record['humidity']}%")

                # Yield for downstream processing
                yield record

        except KeyError as e:
            print(f"❌ Invalid sensor data (missing {e}): {record}")
            continue
        except Exception as e:
            print(f"❌ Error processing sensor data: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
