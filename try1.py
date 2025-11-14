#!/usr/bin/env python3

import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import mysql.connector
from mysql.connector import Error

# ==========================================
# CONFIGURATION
# ==========================================

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Vinita@03',
    'database': 'test',
    'charset': 'utf8mb4'
}

# Processing configuration
BATCH_SIZE = 10
API_ENDPOINT = "https://your-api-endpoint.com/lookup"
API_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 1  # seconds

# Table names
SOURCE_TABLE = "apple_podcast_live_updates"
CHANGES_TABLE = "apple_ids_derived_table"
EMPTY_RESPONSE_TABLE = "apple_live_table_skipped"
CHECKPOINT_TABLE = "apple_ids_derived_index"

# Columns to check for changes
COLUMNS_TO_CHECK = ['column1', 'column2', 'column3', 'status', 'value']

# ==========================================
# LOGGING SETUP
# ==========================================

log_filename = f"batch_processor_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
# GLOBAL VARIABLES
# ==========================================

last_processed_id = 0
shutdown_requested = False

# ==========================================
# SIGNAL HANDLING
# ==========================================

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.warning(f"Received shutdown signal ({signum}). Will stop after current batch.")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ==========================================
# DATABASE FUNCTIONS
# ==========================================

def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logger.info("Database connected successfully")
            return connection
    except Error as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

def ensure_tables_exist(cursor):
    """Create required tables if they don't exist"""
    
    # Table B: Changes table
    create_changes_table = f"""
    CREATE TABLE IF NOT EXISTS `{CHANGES_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        changes_detected TEXT NOT NULL,
        detected_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_detected_at (detected_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    # Table D: Empty responses table
    create_empty_table = f"""
    CREATE TABLE IF NOT EXISTS `{EMPTY_RESPONSE_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_record_id INT UNSIGNED NOT NULL,
        reason VARCHAR(255) DEFAULT NULL,
        recorded_at DATETIME NOT NULL,
        INDEX idx_source_id (source_record_id),
        INDEX idx_recorded_at (recorded_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    # Table C: Checkpoint table
    create_checkpoint_table = f"""
    CREATE TABLE IF NOT EXISTS `{CHECKPOINT_TABLE}` (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        last_processed_id INT UNSIGNED NOT NULL,
        last_run_date DATETIME NOT NULL,
        INDEX idx_last_run (last_run_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    
    try:
        cursor.execute(create_changes_table)
        cursor.execute(create_empty_table)
        cursor.execute(create_checkpoint_table)
        logger.info("All required tables verified/created successfully")
        return True
    except Error as e:
        logger.error(f"Failed to create tables: {e}")
        return False

def get_last_checkpoint(cursor) -> int:
    """Retrieve the last processed ID from checkpoint table"""
    try:
        query = f"""
        SELECT last_processed_id, last_run_date 
        FROM `{CHECKPOINT_TABLE}` 
        ORDER BY id DESC 
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            last_id, last_run = result
            logger.info(f"Found checkpoint: Last ID = {last_id}, Last Run = {last_run}")
            return last_id
        else:
            logger.info("No checkpoint found, starting from beginning")
            return 0
    except Error as e:
        logger.error(f"Error fetching checkpoint: {e}")
        return 0

def save_checkpoint(cursor, connection, last_id: int) -> bool:
    """Save the current processing checkpoint"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = f"""
        INSERT INTO `{CHECKPOINT_TABLE}` (last_processed_id, last_run_date) 
        VALUES (%s, %s)
        """
        cursor.execute(query, (last_id, now))
        connection.commit()
        logger.info(f"Checkpoint saved: Last processed ID = {last_id}")
        return True
    except Error as e:
        logger.error(f"Failed to save checkpoint: {e}")
        return False

def fetch_batch_records(cursor, last_id: int, batch_size: int, target_date: str) -> List[Dict]:
    """Fetch a batch of records from the source table"""
    try:
        columns = ', '.join(['id'] + COLUMNS_TO_CHECK)
        
        # Modify date condition based on your date column name
        query = f"""
        SELECT {columns} 
        FROM `{SOURCE_TABLE}` 
        WHERE id > %s 
        AND DATE(created_date) = %s 
        ORDER BY id ASC 
        LIMIT %s
        """
        
        cursor.execute(query, (last_id, target_date, batch_size))
        
        # Fetch all rows and convert to list of dicts
        columns_list = [desc[0] for desc in cursor.description]
        records = []
        
        for row in cursor.fetchall():
            record = dict(zip(columns_list, row))
            records.append(record)
        
        logger.info(f"Fetched {len(records)} records from ID {last_id}")
        return records
        
    except Error as e:
        logger.error(f"Failed to fetch batch records: {e}")
        return []

# ==========================================
# API FUNCTIONS
# ==========================================

def call_lookup_api(record_id: int, record_data: Dict) -> Dict[str, Any]:
    """Call the lookup API for a given record"""
    try:
        payload = {
            'id': record_id,
            'data': record_data
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        response = requests.post(
            API_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=API_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning(f"API returned HTTP {response.status_code} for ID {record_id}")
            return {
                'success': False, 
                'error': f'HTTP {response.status_code}',
                'http_code': response.status_code
            }
        
        if not response.text or response.text.strip() == '':
            logger.warning(f"Empty response from API for ID {record_id}")
            return {'success': True, 'empty': True}
        
        data = response.json()
        return {'success': True, 'data': data, 'empty': False}
        
    except requests.exceptions.Timeout:
        logger.error(f"API timeout for ID {record_id}")
        return {'success': False, 'error': 'Timeout'}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed for ID {record_id}: {e}")
        return {'success': False, 'error': str(e)}
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response for ID {record_id}")
        return {'success': False, 'error': 'Invalid JSON'}

# ==========================================
# CHANGE DETECTION FUNCTIONS
# ==========================================

def detect_changes(old_record: Dict, new_data: Dict, columns: List[str]) -> Dict[str, Dict]:
    """Detect changes between old and new data"""
    changes = {}
    
    for column in columns:
        old_value = old_record.get(column)
        new_value = new_data.get(column)
        
        # Compare values (handles None, string, numeric)
        if old_value != new_value:
            changes[column] = {
                'old': old_value,
                'new': new_value
            }
    
    return changes

def save_changes_to_table_b(cursor, connection, record_id: int, changes: Dict) -> bool:
    """Save detected changes to Table B"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        changes_json = json.dumps(changes)
        
        query = f"""
        INSERT INTO `{CHANGES_TABLE}` 
        (source_record_id, changes_detected, detected_at) 
        VALUES (%s, %s, %s)
        """
        
        cursor.execute(query, (record_id, changes_json, now))
        connection.commit()
        
        logger.info(f"Changes saved for ID {record_id}: {len(changes)} columns changed")
        return True
        
    except Error as e:
        logger.error(f"Failed to save changes for ID {record_id}: {e}")
        return False

def save_empty_response_to_table_d(cursor, connection, record_id: int, reason: str = 'Empty API response') -> bool:
    """Save empty response record to Table D"""
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        query = f"""
        INSERT INTO `{EMPTY_RESPONSE_TABLE}` 
        (source_record_id, reason, recorded_at) 
        VALUES (%s, %s, %s)
        """
        
        cursor.execute(query, (record_id, reason, now))
        connection.commit()
        
        logger.info(f"Empty response recorded for ID {record_id}")
        return True
        
    except Error as e:
        logger.error(f"Failed to save empty response for ID {record_id}: {e}")
        return False

# ==========================================
# MAIN PROCESSING FUNCTION
# ==========================================

def process_batch(connection, target_date: str) -> bool:
    """Main batch processing function"""
    global last_processed_id, shutdown_requested
    
    logger.info("=== STARTING BATCH PROCESSING ===")
    logger.info(f"Target Date: {target_date}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    
    cursor = connection.cursor()
    
    # Ensure tables exist
    if not ensure_tables_exist(cursor):
        logger.error("Failed to verify/create required tables")
        return False
    
    # Get last checkpoint
    last_processed_id = get_last_checkpoint(cursor)
    
    total_processed = 0
    total_changes = 0
    total_empty = 0
    total_errors = 0
    
    try:
        while not shutdown_requested:
            # Fetch batch
            records = fetch_batch_records(cursor, last_processed_id, BATCH_SIZE, target_date)
            
            if not records:
                logger.info("No more records to process")
                break
            
            logger.info(f"Processing batch of {len(records)} records")
            
            for record in records:
                if shutdown_requested:
                    logger.warning("Shutdown requested, stopping after current record")
                    break
                
                record_id = record['id']
                last_processed_id = record_id
                total_processed += 1
                
                # Call API
                api_result = call_lookup_api(record_id, record)
                
                if not api_result['success']:
                    error_msg = api_result.get('error', 'Unknown')
                    logger.error(f"API call failed for ID {record_id}: {error_msg}")
                    total_errors += 1
                    continue
                
                # Check for empty response
                if api_result.get('empty', False):
                    save_empty_response_to_table_d(cursor, connection, record_id, 'Empty API response')
                    total_empty += 1
                    continue
                
                # Detect changes
                changes = detect_changes(record, api_result['data'], COLUMNS_TO_CHECK)
                
                if changes:
                    save_changes_to_table_b(cursor, connection, record_id, changes)
                    total_changes += 1
                else:
                    logger.info(f"No changes detected for ID {record_id}")
                
                # Delay between requests
                if DELAY_BETWEEN_REQUESTS > 0:
                    time.sleep(DELAY_BETWEEN_REQUESTS)
            
            # Save checkpoint after each batch
            save_checkpoint(cursor, connection, last_processed_id)
            
            logger.info(f"Batch completed. Progress: Processed={total_processed}, Changes={total_changes}, Empty={total_empty}, Errors={total_errors}")
            
            if shutdown_requested:
                break
        
        # Final checkpoint save
        save_checkpoint(cursor, connection, last_processed_id)
        
        logger.info("=== PROCESSING COMPLETE ===")
        logger.info(f"Total Processed: {total_processed}")
        logger.info(f"Changes Detected: {total_changes}")
        logger.info(f"Empty Responses: {total_empty}")
        logger.info(f"Errors: {total_errors}")
        logger.info(f"Last Processed ID: {last_processed_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        save_checkpoint(cursor, connection, last_processed_id)
        return False
    finally:
        cursor.close()

# ==========================================
# MAIN ENTRY POINT
# ==========================================

def main():
    """Main entry point"""
    global last_processed_id
    
    logger.info("=== BATCH LOOKUP PROCESSOR STARTED ===")
    
    # Get target date from command line or use today
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    # Connect to database
    connection = get_db_connection()
    
    try:
        process_batch(connection, target_date)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        cursor = connection.cursor()
        save_checkpoint(cursor, connection, last_processed_id)
        cursor.close()
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    logger.info("Script finished")

if __name__ == "__main__":
    main()