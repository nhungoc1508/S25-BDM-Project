from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp, current_date, lit, col
import json
from datetime import datetime
import os
import uuid
import glob
from pymongo import MongoClient, UpdateOne
from pypdf import PdfReader

# from preprocess_reports import preprocessing_pipeline

MONGO_URI = 'mongodb://root:root@counseling-db:27017/counseling?authSource=admin'
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['counseling']
reports_collection = db['meeting-reports']

BASE_LANDING_PATH = '/data/landing'

TMP_PATH = f'/data/tmp/counseling_reports'
os.makedirs(TMP_PATH, exist_ok=True)

def extract_text(row):
    path, content, _, _, _, _ = row
    filename = path.split('/')[-1]
    filepath = f'{TMP_PATH}/{filename}'
    with open(filepath, 'wb') as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())

def parse_pdf(row):
    """
    Parse text from binary content of a row (= 1 PDF file)
    """
    path, content, _, _, _, _ = row
    filename = path.split('/')[-1]
    filepath = f'{TMP_PATH}/{filename}'
    
    reader = PdfReader(filepath)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    
    return text

def process_text(text):
    """
    Process text and return JSON object
    """
    lines = text.split('\n')
    headings = []
    bulletpoints = []
    for i, line in enumerate(lines):
        if line.isupper():
            headings.append({
                'content': line.strip(),
                'line': i
            })
        elif line.startswith('\x7f'):
            bulletpoints.append(i)
    
    headings.append({
        'content': '_END',
        'line': len(lines)-1
    })
    
    section_contents = dict()
    
    for x in range(len(headings)-1):
        i, j = headings[x]['line'], headings[x+1]['line']
        points = [bp for bp in bulletpoints if bp > i and bp < j]
        if len(points) == 0:
            points.append(i+1)
            points.append(j)
        all_lines = []
        for y in range(len(points)-1):
            n, m = points[y], points[y+1]
            all_lines.append(lines[n:m])
        if m < j:
            all_lines.append(lines[m:j])
        section = headings[x]['content']
        if section != 'GENERAL INFORMATION':
            content = [' '.join(lines).strip().replace('\x7f ', '') for lines in all_lines]
        else:
            content = all_lines[0]
        section_contents[headings[x]['content']] = content
    
    meeting_id, meeting_timestamp, request_id, student_id, counselor_id = None, None, None, None, None
    for line in section_contents['GENERAL INFORMATION']:
        key, value = line.split(': ')
        if key == 'Meeting ID':
            meeting_id = value
        elif key == 'Meeting date':
            meeting_timestamp = datetime.strptime(value, "%B %d, %Y")
        elif key == 'Request ID':
            request_id = value
        elif key == 'Student ID':
            student_id = value
        elif key == 'Counselor ID':
            counselor_id = value
    
    request_obj = {
        'id': meeting_id,
        'request_id': request_id,
        'student_id': student_id,
        'counselor_id': counselor_id,
        'timestamp': meeting_timestamp.isoformat(),
        'meeting_report': dict()
    }
    
    for section, content in section_contents.items():
        if section != 'GENERAL INFORMATION':
            section_title = '_'.join(section.lower().split(' '))
            request_obj['meeting_report'][section_title] = content
    
    return request_obj

def process_row(row):
    text = parse_pdf(row)
    request_obj = process_text(text)
    path, _, source_name, batch_id, ingestion_timestamp, ingestion_date = row
    filename = path.split('/')[-1]
    process_timestamp = datetime.now()
    process_date = process_timestamp.strftime("%Y-%m-%d")
    process_time = process_timestamp.strftime("%H-%M-%S")
    
    request_obj['filename'] = filename
    request_obj['_source_name'] = source_name
    request_obj['_batch_id'] = batch_id
    request_obj['_ingestion_timestamp'] = ingestion_timestamp
    request_obj['_ingestion_date'] = ingestion_date
    request_obj['_process_timestamp'] = process_timestamp.isoformat()
    request_obj['_process_date'] = process_date
    return request_obj

def preprocessing_pipeline(df):
    print('[PROCESSING TASK] preprocessing_pipeline starts')
    rdd = df.select("path", "content", "_source_name", "_batch_id", "_ingestion_timestamp", "_ingestion_date").rdd
    print('[PROCESSING TASK] 0')
    tmp = rdd.map(extract_text)
    print('[PROCESSING TASK] 1')
    _ = tmp.collect()
    print('[PROCESSING TASK] 2')
    pdf_data_rdd = rdd.map(process_row)
    print('[PROCESSING TASK] 3')
    pdf_records = pdf_data_rdd.collect()
    print('[PROCESSING TASK] 4')

    tmp_files = os.listdir(TMP_PATH)
    for tmp_file in tmp_files:
        tmp_filepath = os.path.join(TMP_PATH, tmp_file)
        if os.path.isfile(tmp_filepath):
            os.remove(tmp_filepath)
    print('[PROCESSING TASK] preprocessing_pipeline starts')
    return pdf_records

def init_spark():
    builder = SparkSession.builder \
        .appName("MeetingReportsProcessing") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def read_data(spark, source_name):
    landing_dir = f'{BASE_LANDING_PATH}/{source_name}'
    all_batches = glob.glob(os.path.join(landing_dir, 'date=*', 'batch=*'))
    latest_batch_path = max(all_batches, key=os.path.getmtime)
    abs_path = os.path.abspath(latest_batch_path)
    batch_id = os.path.basename(abs_path).split('=')[1]

    df = spark.read \
        .format('delta') \
        .load(abs_path)

    return df, batch_id

def process_data(df):
    print(f'[PROCESSING TASK] Preprocessing began')
    pdf_records = preprocessing_pipeline(df)
    print(f'[PROCESSING TASK] Preprocessing completed')

    operations = [
        UpdateOne(
            {'id': record['id']},
            {'$set': record},
            upsert=True
        )
        for record in pdf_records
    ]

    result = reports_collection.bulk_write(operations)
    print(f"[PROCESSING TASK] Matched: {result.matched_count}, modified: {result.modified_count}, upserted: {result.upserted_count}")

if __name__ == '__main__':
    spark = init_spark()
    source_name = 'meeting_reports'
    df, batch_id = read_data(spark, source_name)
    process_data(df)
    spark.sparkContext.stop()
    print(f'[PROCESSING TASK] Processing completed for {source_name} batch ID {batch_id}')