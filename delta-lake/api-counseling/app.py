from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import os
from datetime import datetime
import urllib.parse

app = FastAPI()

origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://s25-vbp-prototype-upc.vercel.app"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = 'mongodb://root:root@counseling-db:27017/?authSource=admin'
client = MongoClient(MONGO_URI)

MONGO_URI_OG = 'mongodb://root:root@mongo_counselors:27017/counseling?authSource=admin'
client_og = MongoClient(MONGO_URI_OG)

db = client['counseling']
counselors = db['counselors']
counselors_og = client_og['counseling']['counselors']
requests = db['meeting-requests']
reports = db['meeting-reports']
new_requests = db['new-meeting-requests']

@app.get("/counselors")
def get_counselors():
    counselors_data = list(counselors.find({}, {"_id": 0}))
    return counselors_data

@app.get("/counselors_original")
def get_counselors():
    counselors_data = list(counselors_og.find({}, {"_id": 0}))
    return counselors_data

@app.get("/meeting_requests")
def get_meeting_requests():
    requests_data = list(requests.find({}, {"_id": 0}))
    return requests_data

@app.get("/meeting_requests/{student_id}")
def get_requests_by_student(student_id: str):
    requests_data = list(requests.find({"student_id": student_id}, {"_id": 0}))
    if requests_data:
        return requests_data
    return {"message": "Not found"}

@app.get("/meeting_reports")
def get_meeting_reports():
    reports_data = list(reports.find({}, {"_id": 0}))
    return reports_data

@app.get("/meeting_reports/counselor={counselor_id}")
def get_meetings_by_counselor(counselor_id: str):
    reports_data = list(reports.find({"counselor_id": counselor_id}, {"_id": 0}))
    if reports_data:
        return reports_data
    return {"message": "Not found"}

@app.get("/meeting_reports/student={student_id}")
def get_meetings_by_student(student_id: str):
    reports_data = list(reports.find({"student_id": student_id}, {"_id": 0}))
    if reports_data:
        return reports_data
    return {"message": "Not found"}

@app.get("/new_meeting_requests")
def get_new_meeting_requests():
    requests = list(new_requests.find({}, {"_id": 0}))
    return requests