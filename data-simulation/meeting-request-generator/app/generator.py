import os
import json
import random
import time
import psycopg2
import uuid
import requests
from kafka import KafkaProducer
from faker import Faker
import datetime

# Kafka Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:9092")
KAFKA_TOPIC_REQUESTS = 'meeting-requests'
KAFKA_TOPIC_REPORTS = 'meeting-reports'
MONGO_REQUEST_URL = 'http://fastapi_counselor_data:8000/insert_meeting_request'
MONGO_REPORT_URL = 'http://fastapi_counselor_data:8000/insert_meeting_report'

print(f'Kafka server: {KAFKA_SERVER}')
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)

fake = Faker()

def get_db_connection():
    return psycopg2.connect(
        host='postgres_sis',
        port='5432',
        user='admin',
        password='password',
        database='university'
    )

def load_student_ids():
    student_ids = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT student_id FROM students")
        student_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error loading student IDs: {e}")
    
    return student_ids

def load_courses():
    courses = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT title FROM courses")
        courses = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error loading courses: {e}")
    
    return courses

def load_majors():
    majors = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM departments")
        majors = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error loading majors: {e}")

    return majors

def load_counselor_ids():
    api_url = 'http://fastapi_counselor_data:8000/counselors'
    response = requests.get(api_url)
    json_data = response.json()
    counselor_ids = []
    for obj in json_data:
        counselor_ids.append(obj['counselor_id'])

    return counselor_ids

MEETING_PURPOSES = [
    "Academic progress review",
    "Course selection assistance",
    "Major declaration",
    "Study plan development",
    "Academic probation meeting",
    "Graduation requirements check",
    "Internship application help",
    "Career path guidance",
    "Transfer credit evaluation",
    "Mental health resources",
    "Accommodations request",
    "Research opportunity discussion",
    "Study abroad planning",
    "Financial aid questions",
    "Academic difficulty support"
]

DESCRIPTION_TEMPLATES = [
    "I'm struggling with {course} and need help understanding how to improve my grade.",
    "I want to change my major and need advice on what courses I should take next semester.",
    "I need to review my graduation requirements to make sure I'm on track to graduate in {timeframe}.",
    "I've been dealing with {issue} this semester and it's affecting my academics. I need advice on how to proceed.",
    "I'm interested in studying abroad in {country} and need guidance on how this will affect my graduation timeline.",
    "I'm having trouble balancing my coursework with my job as a {job_title}. I need advice on time management.",
    "I would like to discuss potential career paths for someone with a {major} degree.",
    "I need help selecting courses for next semester that align with my interest in {interest_area}.",
    "I recently transferred from another university and want to discuss how my credits transferred over.",
    "I'm on academic probation and need to create a plan to improve my GPA this semester."
]

def generate_request(student_ids, majors, courses):
    student_id = random.choice(student_ids)
    purpose = random.choice(MEETING_PURPOSES)
    
    template = random.choice(DESCRIPTION_TEMPLATES)
    description = template.format(
        course=random.choice(courses),
        grade=random.choice(["C-", "D+", "F", "B-", "C+"]),
        timeframe=random.choice(["Spring 2025", "Fall 2025", "Spring 2026"]),
        issue=random.choice(["anxiety", "family emergency", "health issues", "work conflicts"]),
        country=fake.country(),
        job_title=fake.job(),
        major=random.choice(majors),
        interest_area=random.choice(["data science", "marketing", "research", "sustainability", "healthcare"])
    )
    
    # Sometimes add extra paragraphs for more realistic variation in description length
    if random.random() < 0.3:
        description += "\n\n" + fake.paragraph()
    
    requestor_type = "student"

    now = datetime.datetime.now()
    
    return {
        "request_id": str(uuid.uuid4()),
        "student_id": student_id,
        "purpose": purpose,
        "description": description,
        "request_timestamp": now.isoformat(),
        "requestor_type": requestor_type
    }

def generate_batch_requests(student_ids, batch_size=20):
    batch = []

    theme = random.choice([
        "Mid-term grade interventions",
        "First-generation student check-in",
        "Financial aid warning follow-up",
        "Academic probation meeting",
        "Graduation requirement check"
    ])
    
    selected_students = random.sample(student_ids, batch_size)
    
    now = datetime.datetime.now()
    
    for student_id in selected_students:
        description = f"Required meeting for {theme.lower()}. Please schedule at your earliest convenience."
        
        batch.append({
            "request_id": str(uuid.uuid4()),
            "student_id": student_id,
            "purpose": theme,
            "description": description,
            "request_timestamp": now.isoformat(),
            "requestor_type": "counselor"
        })
    
    return batch

def generate_report(student_ids, counselor_ids):
    meeting_id = str(uuid.uuid4())
    student_id = random.choice(student_ids)
    counselor_id = random.choice(counselor_ids)
    meeting_date = (datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 90))).isoformat()
    format = random.choice(["in-person", "online"])
    report_content = fake.paragraph(nb_sentences=10)

    return {
        "meeting_id": meeting_id,
        "student_id": student_id,
        "counselor_id": counselor_id,
        "meeting_date": meeting_date,
        "format": format,
        "report_content": report_content
    }

def main():
    print('Starting meeting requests/reports simulation')
    student_ids = load_student_ids()
    print(f'Loaded {len(student_ids)} student IDs')
    courses = load_courses()
    print(f'Loaded {len(courses)} courses')
    majors = load_majors()
    print(f'Loaded {len(majors)} majors')
    counselor_ids = load_counselor_ids()
    print(f'Loaded {len(counselor_ids)} counselor IDs')
    
    try:
        batch_counter = 0
        while True:
            now = datetime.datetime.now()
            delay = 10 # Once every 10 seconds
            if random.random() < 0.6: # Send a meeting request
                request = generate_request(student_ids, majors, courses)
                # producer.send(KAFKA_TOPIC_REQUESTS, request)
                # producer.flush()
                response = requests.post(MONGO_REQUEST_URL, json=request)
                print(f'Sent request: {request}', flush=True)
                batch_counter += 1
                if batch_counter >= 20:
                    batch_counter = 0
                    if random.random() < 0.7:
                        batch_size = random.randint(10, 30)
                        print(f'Generating batch of {batch_size} requests')
                        batch_requests = generate_batch_requests(student_ids, batch_size)
                        for batch_request in batch_requests:
                            # producer.send(KAFKA_TOPIC_REQUESTS, batch_request)
                            # producer.flush()
                            response = requests.post(MONGO_REQUEST_URL, json=batch_request)
                            print(f'Sent request: {batch_request}', flush=True)
                            time.sleep(0.1)
                        print(f'Sent batch of {batch_size} requests')
            else: # Send a meeting report
                report = generate_report(student_ids, counselor_ids)
                # producer.send(KAFKA_TOPIC_REPORTS, report)
                # producer.flush()
                response = requests.post(MONGO_REPORT_URL, json=report)
                print(f'Sent report: {report}', flush=True)
            
            time.sleep(delay)

    except KeyboardInterrupt:
        print('Simulation stopped by user')
    except Exception as e:
        print(f'Error during simulation: {e}')

if __name__ == "__main__":
    main()