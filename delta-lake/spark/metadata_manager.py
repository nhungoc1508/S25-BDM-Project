import json
import os
from datetime import datetime
import fcntl

BASE_PATH = '/data'
METADATA_FILE = f'{BASE_PATH}/metadata/catalog.json'

def initialize():
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    if not os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'w') as f:
            json.dump([], f)
        print(f'[METADATA MANAGER] Initialized metadata file at {METADATA_FILE}')

def read_current_metadata():
    initialize()
    with open(METADATA_FILE, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print('[METADATA MANAGER] Warning: Metadata file is corrupted. Reinitializing.')
            with open(METADATA_FILE, 'w') as f:
                json.dump([], f)
            return []
        
def write_metadata(metadata_list):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, 'w') as f:
        # Use file locking to prevent concurrent writes from multiple DAGs
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            json.dump(metadata_list, f, indent=2)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

def add_metadata_entry(entry):
    metadata_list = read_current_metadata()
    metadata_list.append(entry)
    write_metadata(metadata_list)
    print(f'[METADATA MANAGER] Added metadata entry for batch {entry.get("batch_id")}')

def update_metadata_entry(batch_id, updates):
    metadata_list = read_current_metadata()
    
    for i, entry in enumerate(metadata_list):
        if entry.get('batch_id') == batch_id:
            metadata_list[i].update(updates)
            write_metadata(metadata_list)
            print(f'[METADATA MANAGER] Updated metadata for batch {batch_id}')
            return True
    
    print(f'[METADATA MANAGER] Warning: Batch {batch_id} not found in metadata')
    return 

def get_batches_by_status(status):
    metadata_list = read_current_metadata()
    return [entry for entry in metadata_list if entry.get('process_status') == status]

def generate_metadata_summary():
    metadata_list = read_current_metadata()
    
    summary = {}
    for entry in metadata_list:
        source = entry.get('source_name')
        date = entry.get('ingestion_date', '')
        month = date[:7] if len(date) >= 7 else 'unknown'
        status = entry.get('process_status')
        record_count = entry.get('record_count', 0)
        
        key = f"{source}_{month}"
        if key not in summary:
            summary[key] = {
                'source_name': source,
                'month': month,
                'batch_count': 0,
                'total_records': 0,
                'promoted_batches': 0,
                'failed_batches': 0,
                'pending_batches': 0
            }
        
        summary[key]['batch_count'] += 1
        summary[key]['total_records'] += record_count
        
        if status == 'PROMOTED_TO_PERSISTENT':
            summary[key]['promoted_batches'] += 1
        elif status == 'PROMOTION_FAILED':
            summary[key]['failed_batches'] += 1
        elif status == 'INGESTED_TO_TEMPORAL':
            summary[key]['pending_batches'] += 1
    
    summary_list = list(summary.values())
    summary_list.sort(key=lambda x: (x['source_name'], x['month']), reverse=True)
    
    timestamp = datetime.now()
    date = timestamp.strftime("%Y-%m-%d")
    time = timestamp.strftime("%H-%M-%S")
    summary_file = f"{BASE_PATH}/metadata/summary_{date}_{time}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary_list, f, indent=2)
    
    print(f'[METADATA MANAGER] Generated metadata summary at {summary_file}')
    return summary_list

def update_metadata_for_promotion(batch_id, persistent_path):
    updates = {
        "persistent_path": persistent_path,
        "promotion_timestamp": datetime.now().isoformat(),
        "process_status": "PROMOTED_TO_PERSISTENT"
    }
    print(f'[METADATA MANAGER] Successfully updated metadata for promotion of batch at {persistent_path}')
    return update_metadata_entry(batch_id, updates)

def update_metadata_for_failed_promotion(batch_id, error_message):
    updates = {
        "process_status": "PROMOTION_FAILED",
        "error_message": error_message,
        "error_timestamp": datetime.now().isoformat()
    }
    
    return update_metadata_entry(batch_id, updates)

def get_last_timestamp(source_name):
    metadata_list = read_current_metadata()
    for entry in metadata_list[::-1]:
        if entry.get('source_name') == source_name and entry.get('process_status') == 'INGESTED_TO_TEMPORAL':
            return entry.get('last_timestamp')
    return None