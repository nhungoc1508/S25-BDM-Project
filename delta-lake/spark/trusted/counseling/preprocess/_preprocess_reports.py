from pyspark.sql.functions import col, lit, struct, array, expr
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    concat_ws, lower, col, current_date, to_timestamp, datediff, lit, size, array_distinct
)

landing_dir = 'file:///data/landing/meeting_reports/date=2025-05-27/batch=a4284baf-04e4-457b-8714-4fca01c3050b'
df = spark.read.format('delta').load(landing_dir)

doc_path = 'file:/data/tmp/counseling_reports/8742cf5f-aa27-4ac1-9522-9f1c2f6f32d1_report.pdf'
pdf_bytes = df.first().content

pdf_file_path = '/data/tmp/counseling_reports/tmp.pdf'

with open(pdf_file_path, 'wb') as f:
    f.write(pdf_bytes)

from pypdf import PdfReader
reader = PdfReader(pdf_file_path)
text = ""
for page in reader.pages:
    text += page.extract_text() + "\n"

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

print(meeting_id, meeting_timestamp, request_id, student_id, counselor_id)

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

MONGO_URI = 'mongodb://root:root@counseling-db:27017/counseling?authSource=admin'
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['counseling']
reports_collection = db['meeting-reports']

reports_collection.insert_many([request_obj])

rdd = df.select("path", "content", "_source_name", "_batch_id", "_ingestion_timestamp", "_ingestion_date").rdd
pdf_data_rdd = rdd.map(process_row)
pdf_records = pdf_data_rdd.collect()

from pyspark.sql.types import StructType
from pyspark.sql.functions import col, struct, array, lower, to_timestamp
from pypdf import PdfReader

'file:/data/tmp/counseling_reports/8a7e7d11-cfec-4546-ba57-d2757ae1322b_report'