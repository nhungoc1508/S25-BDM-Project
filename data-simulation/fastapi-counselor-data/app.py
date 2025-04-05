from fastapi import FastAPI, Request, HTTPException
from pymongo import MongoClient
import os
from datetime import datetime
import urllib.parse

app = FastAPI()

MONGO_URI = 'mongodb://root:root@mongo_counselors:27017/counseling?authSource=admin'
client = MongoClient(MONGO_URI)

# client = MongoClient(os.getenv('MONGO_URI', 'mongodb://root:root@mongo_counselors:27017'))
db = client['counseling']
counselors = db['counselors']
requests = db['meeting_requests']
reports = db['meeting_reports']

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

@app.get("/meeting_requests")
def get_meeting_requests():
    requests_data = list(requests.find({}, {"_id": 0}))
    return requests_data

@app.get("/meeting_requests/{encoded_timestamp}")
def get_meeting_requests_after(encoded_timestamp: str):
    decoded_timestamp = ''
    try:
        decoded_timestamp = str(urllib.parse.unquote(encoded_timestamp))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")
    requests_data = list(requests.find({'request_timestamp': { "$gt" : decoded_timestamp }},  {"_id": 0}))
    return requests_data

@app.get("/meeting_reports")
def get_meeting_reports():
    reports_data = list(reports.find({}, {"_id": 0}))
    return reports_data

@app.get("/meeting_reports/{encoded_timestamp}")
def get_meeting_reportss_after(encoded_timestamp: str):
    decoded_timestamp = ''
    try:
        decoded_timestamp = str(urllib.parse.unquote(encoded_timestamp))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")
    reports_data = list(requests.find({'meeting_date': { "$gt" : decoded_timestamp }},  {"_id": 0}))
    return reports_data

@app.post("/insert_meeting_request")
async def insert_meeting_request(request: Request):
    try:
        data = await request.json()
        print(f'Received request with data: {data}')
        print('Inserting now')
        try:
            result = requests.insert_one(data)
            print(f'Inserted with ID: {str(result.inserted_id)}')
            return {"inserted_id": str(result.inserted_id)}
        except Exception as e:
            print(f'Cannot insert: {e}')
            return {"message": f"Cannot insert: {e}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/insert_meeting_report")
async def insert_meeting_report(request: Request):
    try:
        data = await request.json()
        print(f'Received report with data: {data}')
        print('Inserting now')
        try:
            result = reports.insert_one(data)
            print(f'Inserted with ID: {str(result.inserted_id)}')
            return {"inserted_id": str(result.inserted_id)}
        except Exception as e:
            print(f'Cannot insert: {e}')
            return {"message": f"Cannot insert: {e}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))