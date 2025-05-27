import duckdb
from datetime import datetime
import pandas as pd

BASE_PATH = '/opt/airflow/spark/trusted/databases'
DUCKDB_PATH = f'{BASE_PATH}/trusted_data.db'

# Ensure DB and connection
def get_connection():
    return duckdb.connect(DUCKDB_PATH)

def initialize():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata_catalog (
                source_name TEXT,
                batch_id TEXT PRIMARY KEY,
                ingestion_timestamp TIMESTAMP,
                ingestion_date TEXT,
                record_count INTEGER,
                temporal_path TEXT,
                trusted_zone_path TEXT,
                persistent_path TEXT,
                process_status TEXT,
                error_message TEXT,
                promotion_timestamp TIMESTAMP,
                error_timestamp TIMESTAMP
            );
        """)
        print(f'[METADATA MANAGER] Initialized metadata table at {DUCKDB_PATH}')

def add_metadata_entry(entry):
    initialize()
    with get_connection() as conn:
        # conn.execute("DELETE FROM metadata_catalog WHERE source_name = 'sis_evaluations'")
        conn.execute("""
            INSERT INTO metadata_catalog (
                source_name, batch_id, ingestion_timestamp, ingestion_date,
                record_count, temporal_path, trusted_zone_path, persistent_path,
                process_status, error_message, promotion_timestamp, error_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.get("source_name"),
            entry.get("batch_id"),
            entry.get("ingestion_timestamp"),
            entry.get("ingestion_date"),
            entry.get("record_count"),
            entry.get("temporal_path"),
            entry.get("trusted_zone_path"),
            entry.get("persistent_path"),
            entry.get("process_status"),
            entry.get("error_message"),
            entry.get("promotion_timestamp"),
            entry.get("error_timestamp"),
        ))
    print(f'[METADATA MANAGER] Added metadata entry for batch {entry.get("batch_id")}')

def update_metadata_entry(batch_id, updates):
    initialize()
    set_clause = ', '.join([f"{key} = ?" for key in updates])
    values = list(updates.values())
    values.append(batch_id)

    with get_connection() as conn:
        conn.execute(f"""
            UPDATE metadata_catalog
            SET {set_clause}
            WHERE batch_id = ?
        """, values)
    print(f'[METADATA MANAGER] Updated metadata for batch {batch_id}')
    return True

def get_batches_by_status(status):
    initialize()
    with get_connection() as conn:
        result = conn.execute("""
            SELECT * FROM metadata_catalog
            WHERE process_status = ?
            ORDER BY ingestion_timestamp DESC
        """, (status,)).fetchall()
    return result

def generate_metadata_summary():
    initialize()
    with get_connection() as conn:
        try:
            result = conn.execute("""
                SELECT 
                    source_name,
                    strftime(ingestion_date, '%Y-%m') AS month,
                    COUNT(*) AS batch_count,
                    SUM(record_count) AS total_records,
                    SUM(CASE WHEN process_status = 'PROMOTED_TO_PERSISTENT' THEN 1 ELSE 0 END) AS promoted_batches,
                    SUM(CASE WHEN process_status = 'PROMOTION_FAILED' THEN 1 ELSE 0 END) AS failed_batches,
                    SUM(CASE WHEN process_status = 'INGESTED_TO_TEMPORAL' THEN 1 ELSE 0 END) AS pending_batches
                FROM metadata_catalog
                GROUP BY source_name, month
                ORDER BY source_name, month DESC
            """).fetchall()

            # Convert to DataFrame for easier manipulation
            columns = ['source_name', 'month', 'batch_count', 'total_records',
                       'promoted_batches', 'failed_batches', 'pending_batches']
            result_df = pd.DataFrame(result, columns=columns)

            # Instead of saving it to CSV, return the DataFrame directly
            print(f'[METADATA MANAGER] Generated metadata summary.')
            return result_df
        except Exception as e:
            print(f'[ERROR] Failed to generate metadata summary: {str(e)}')
            return pd.DataFrame()

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
    initialize()
    with get_connection() as conn:
        result = conn.execute("""
            SELECT ingestion_timestamp
            FROM metadata_catalog
            WHERE source_name = ? AND process_status = 'INGESTED_TO_TEMPORAL'
            ORDER BY ingestion_timestamp DESC
            LIMIT 1
        """, (source_name,)).fetchone()
    return result[0] if result else None
