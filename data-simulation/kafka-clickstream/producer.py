from kafka import KafkaProducer
import json
import random
from faker import Faker

faker = Faker()
producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))

while True:
    event = {
        "student_id": random.randint(1, 1000),
        "action": random.choice(["login", "view_assignment", "submit_quiz"]),
        "timestamp": faker.date_time().isoformat(),
        "page": faker.uri()
    }
    producer.send("clickstream", value=event)