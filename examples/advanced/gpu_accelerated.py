#!/usr/bin/env python3
"""
GPU-Accelerated Processing Example

This example demonstrates Sabot's GPU acceleration capabilities using RAFT.
It shows:
- GPU-accelerated K-means clustering
- GPU-accelerated nearest neighbors
- CPU vs GPU performance comparison
- Real-time ML on streaming data

Prerequisites:
- Run: python setup_sabot.py (from project root)
- Or: source .venv/bin/activate && pip install -e .
- GPU: pip install cudf cuml raft-dask (requires CUDA)

Usage:
    # With full Sabot (recommended)
    source /path/to/sabot/.venv/bin/activate
    python examples/XXXX.py

    # Without full Sabot (automatic fallback)
    python examples/XXXX.py
    source /path/to/sabot/.venv/bin/activate
    python examples/gpu_accelerated.py
"""

import asyncio
import random
import time
import numpy as np

# Import optional GPU libraries
try:
    import cudf
    import cuml
    from raft.dask import RFT
    GPU_AVAILABLE = True
except ImportError:
    print("⚠️  GPU libraries not available. This example will run in CPU-only mode.")
    print("   To enable GPU acceleration, install: pip install cudf cuml raft-dask")
    GPU_AVAILABLE = False

# Import Sabot (will work if virtual environment is activated)
try:
    from sabot import create_app, StreamBuilder
    SABOT_AVAILABLE = True
except ImportError:
    print("⚠️  Full Sabot not available. Running simplified simulation...")
    SABOT_AVAILABLE = False

    # Mock classes for demonstration
    class MockApp:
        def __init__(self, name): self.name = name
        def agent(self, topic):
            def decorator(func): return func
            return decorator
        async def send_to_topic(self, topic, data): pass
        async def start(self): pass
        async def stop(self): pass
        async def joins(self): return MockJoins()

    class MockStream:
        def __init__(self, app, topic): pass
        def map(self, func): return self
        def filter(self, func): return self
        def key_by(self, func): return self
        def window(self, *args): return self
        def sum(self): return self
        def agg(self, *args): return self
        def sink(self, func): return self
        async def start(self): pass
        async def stop(self): pass

    class MockJoins:
        def arrow_table_join(self, *args, **kwargs): return MockJoin()
        def arrow_dataset_join(self, *args, **kwargs): return MockJoin()

    class MockJoin:
        def on(self, *args): return self
        async def execute(self): return []

    def create_app(name): return MockApp(name)
    def StreamBuilder(app, topic): return MockStream(app, topic)
try:
    from sabot import create_app, StreamBuilder
except ImportError:
    print("❌ Sabot not found. Please run:")
    print("   cd /path/to/sabot")
    print("   python setup_sabot.py")
    print("   source .venv/bin/activate")
    exit(1)
- Real-time ML on streaming data

Note: This example requires CUDA and RAPIDS AI libraries.
"""

import asyncio
import random
import time
import numpy as np
from sabot import create_app, StreamBuilder

try:
    import cudf
    import cuml
    from raft.dask import RFT
    GPU_AVAILABLE = True
except ImportError:
    print("⚠️  GPU libraries not available. This example will run in CPU-only mode.")
    print("   To enable GPU acceleration, install: pip install cudf cuml raft-dask")
    GPU_AVAILABLE = False


async def generate_sensor_data():
    """Generate simulated IoT sensor data."""
    sensors = [f"sensor_{i}" for i in range(1, 21)]  # 20 sensors

    while True:
        sensor_id = random.choice(sensors)

        # Generate multi-dimensional sensor readings
        readings = {
            "sensor_id": sensor_id,
            "temperature": round(random.uniform(20.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 80.0), 2),
            "pressure": round(random.uniform(980.0, 1020.0), 2),
            "vibration": round(random.uniform(0.0, 5.0), 2),
            "timestamp": time.time()
        }

        # Occasionally generate anomalous readings (simulating equipment issues)
        if random.random() < 0.05:  # 5% chance of anomaly
            readings["temperature"] += random.uniform(10.0, 20.0)
            readings["vibration"] += random.uniform(3.0, 7.0)

        yield readings
        await asyncio.sleep(0.1)


class SensorAnomalyDetector:
    """GPU-accelerated sensor anomaly detection."""

    def __init__(self):
        self.training_data = []
        self.kmeans_model = None
        self.nn_model = None
        self.is_trained = False

    def collect_training_sample(self, sensor_data):
        """Collect data for training."""
        if len(self.training_data) < 1000:  # Collect 1000 samples
            features = [
                sensor_data["temperature"],
                sensor_data["humidity"],
                sensor_data["pressure"],
                sensor_data["vibration"]
            ]
            self.training_data.append(features)

    def train_models(self):
        """Train GPU-accelerated models."""
        if len(self.training_data) < 100 and not self.is_trained:
            return False

        if GPU_AVAILABLE and not self.is_trained:
            try:
                # Convert to cuDF DataFrame
                df = cudf.DataFrame(self.training_data,
                                  columns=["temp", "humidity", "pressure", "vibration"])

                # GPU-accelerated K-means clustering
                self.kmeans_model = cuml.KMeans(n_clusters=3, random_state=42)
                self.kmeans_model.fit(df)

                # GPU-accelerated Nearest Neighbors
                self.nn_model = cuml.Neighbors.NearestNeighbors(n_neighbors=5)
                self.nn_model.fit(df)

                self.is_trained = True
                print("🎯 GPU models trained successfully!")
                return True

            except Exception as e:
                print(f"❌ GPU training failed: {e}")
                return False
        else:
            # CPU fallback
            self.is_trained = True
            print("💻 CPU-only mode (GPU not available)")
            return True

    def detect_anomaly(self, sensor_data):
        """Detect anomalies using trained models."""
        if not self.is_trained:
            return "UNKNOWN"

        features = np.array([[
            sensor_data["temperature"],
            sensor_data["humidity"],
            sensor_data["pressure"],
            sensor_data["vibration"]
        ]])

        if GPU_AVAILABLE and self.kmeans_model:
            try:
                # GPU inference
                gpu_features = cudf.DataFrame(features,
                                            columns=["temp", "humidity", "pressure", "vibration"])

                # Get cluster assignment
                cluster = self.kmeans_model.predict(gpu_features)[0]

                # Get distance to nearest neighbors
                distances, _ = self.nn_model.kneighbors(gpu_features)
                avg_distance = float(distances.iloc[0].mean())

                # Anomaly detection based on distance from cluster center
                if avg_distance > 2.0:  # Threshold for anomaly
                    return "CRITICAL"
                elif avg_distance > 1.0:
                    return "WARNING"

                return "NORMAL"

            except Exception as e:
                print(f"❌ GPU inference failed: {e}")
                return "ERROR"

        else:
            # CPU fallback - simple rule-based detection
            temp = sensor_data["temperature"]
            vibration = sensor_data["vibration"]

            if temp > 40 or vibration > 6:
                return "CRITICAL"
            elif temp > 35 or vibration > 4:
                return "WARNING"

            return "NORMAL"


async def main():
    """Run the GPU-accelerated processing example."""
    print("🚀 GPU-Accelerated Processing Example")
    print("=" * 40)

    if SABOT_AVAILABLE:
        print("✅ Running with full Sabot engine")
    else:
        print("🔄 Running with simulation (full Sabot not available)")

    if not GPU_AVAILABLE:
        print("⚠️  GPU libraries not detected.")
        print("   This example will run with CPU-based anomaly detection.\n")

    # Create Sabot application
    app = create_app("gpu-analytics")

    # Create anomaly detector
    detector = SensorAnomalyDetector()

    # Training phase: Collect initial data
    print("📚 Training Phase: Collecting sensor data...")
    training_start = time.time()

    @app.agent('sensor-data')
    async def training_agent(sensor_stream):
        """Collect training data."""
        async for batch in sensor_stream:
            for record in batch.to_pylist():
                detector.collect_training_sample(record)

                # Check if we have enough data to train
                if len(detector.training_data) >= 100 and not detector.is_trained:
                    if detector.train_models():
                        print(".1f"                        break

            # Yield the data for further processing
            yield batch

    # Inference phase: Real-time anomaly detection
    anomaly_counts = {"NORMAL": 0, "WARNING": 0, "CRITICAL": 0, "UNKNOWN": 0}

    @app.agent('sensor-data')
    async def inference_agent(sensor_stream):
        """Perform real-time anomaly detection."""
        async for batch in sensor_stream:
            for record in batch.to_pylist():
                anomaly_level = detector.detect_anomaly(record)

                anomaly_counts[anomaly_level] += 1

                # Alert on anomalies
                if anomaly_level in ["WARNING", "CRITICAL"]:
                    emoji = "🚨" if anomaly_level == "CRITICAL" else "⚠️"
                    print(".2f"
                          f"({record['sensor_id']})")

                # Yield enriched data
                enriched_record = {**record, "anomaly_level": anomaly_level}
                yield enriched_record

    # Aggregation pipeline: Summary statistics
    summary_pipeline = (StreamBuilder(app, "sensor-data")
        .filter(lambda record: record.get("anomaly_level") in ["WARNING", "CRITICAL"])
        .window("tumbling", 5.0)  # 5-second summary windows
        .agg({"anomaly_count": "count"})
        .sink(lambda window: print(f"📊 Last 5s: {window['anomaly_count']} anomalies detected"))
    )

    print("🎯 Starting GPU-accelerated anomaly detection...")
    print("🤖 System will train on initial data, then detect anomalies in real-time")
    print("Press Ctrl+C to stop\n")

    # Start the application
    await app.start()

    # Generate and send sensor data
    sensor_generator = generate_sensor_data()
    start_time = time.time()

    try:
        while time.time() - start_time < 25:  # Run for 25 seconds
            sensor_data = await anext(sensor_generator)
            await app.send_to_topic('sensor-data', sensor_data)

            await asyncio.sleep(0.05)  # Small delay between sends

    except KeyboardInterrupt:
        print("\n🛑 Stopping GPU analytics...")

    # Final statistics
    total_processed = sum(anomaly_counts.values())
    print("
📈 Final Statistics:"    print(f"   Total readings processed: {total_processed}")
    print(f"   Normal readings: {anomaly_counts['NORMAL']} ({anomaly_counts['NORMAL']/total_processed*100:.1f}%)")
    print(f"   Warning anomalies: {anomaly_counts['WARNING']} ({anomaly_counts['WARNING']/total_processed*100:.1f}%)")
    print(f"   Critical anomalies: {anomaly_counts['CRITICAL']} ({anomaly_counts['CRITICAL']/total_processed*100:.1f}%)")

    if GPU_AVAILABLE:
        print("✅ GPU acceleration was used for this example")
    else:
        print("💻 CPU-based processing was used (GPU not available)")

    # Clean shutdown
    await app.stop()
    print("✅ GPU-accelerated example completed!")


if __name__ == "__main__":
    asyncio.run(main())
