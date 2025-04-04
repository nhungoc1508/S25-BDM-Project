import os
import json
import time
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

# Kafka Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "counselor-requests")

print(KAFKA_SERVER)
# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)

# Initialize Faker
fake = Faker()

def generate_meeting_request():
    """Generate a fake meeting request event."""
    return {
        "student_id": fake.uuid4(),
        "purpose": fake.sentence(nb_words=6),
        "description": fake.paragraph(nb_sentences=3),
        "requested_at": datetime.now().isoformat()
    }

if __name__ == "__main__":
    print("Starting Kafka meeting request generator...")
    while True:
        message = generate_meeting_request()
        print(f"Generated: {message}", flush=True)
        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        time.sleep(5)