from fastapi import FastAPI
from pymongo import MongoClient
import os

app = FastAPI()

client = MongoClient(os.getenv('MONGO_URI', 'mongodb://root:root@mongo_counselors:27017'))
db = client['counseling']
counselors = db['counselors']

@app.get("/counselors")
def get_counselors():
    counselors_data = list(counselors.find({}, {"_id": 0}))  # Exclude MongoDB internal ID
    return counselors_data

@app.get("/counselor/{counselor_id}")
def get_counselor(counselor_id: str):
    counselor_data = counselors.find_one({"counselor_id": counselor_id}, {"_id": 0})
    if counselor_data:
        return counselor_data
    return {"message": "Counselor not found"}