from fastapi import FastAPI
import random
from faker import Faker

faker = Faker()
app = FastAPI()

@app.get("/appointments")
def get_appointments():
    return [
        {"student_id": random.randint(1, 1000), "counselor_id": random.randint(1, 50), "date": faker.date_this_year(), "purpose": faker.sentence()}
        for _ in range(100)
    ]