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
import password
import os
# uploaded on crawler by name try6.py
# Database configuration
DB_CONFIG = {
    'host': password.host,
    'user': password.user,
    'password': password.password,
    'database': password.database,
    'charset': 'utf8mb4'
}

# Processing configuration
BATCH_SIZE = 10  # Number of records to process before exit
API_ENDPOINT = "https://itunes.apple.com/lookup"
API_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5

# Table names (assumed to already exist in the DB)
SOURCE_TABLE = "apple_podcast_live_updates"
CHANGES_TABLE = "apple_ids_derived_table"  # Stores updated values
EMPTY_RESPONSE_TABLE = "apple_live_table_skipped"
CHECKPOINT_TABLE = "apple_ids_derived_index"

# Column mapping: Source DB column -> Changes table column -> API response field
COLUMN_MAPPING = {
    'apple_podcast_url': {'changes_col': 'applePodcastUrl', 'api_field': 'collectionViewUrl'},
    'category': {'changes_col': 'podcastCategory', 'api_field': 'primaryGenreName'},
    'image': {'changes_col': 'imageUrl', 'api_field': 'artworkUrl30'},
    'name': {'changes_col': 'podcastName', 'api_field': 'collectionName'},
    'artistName': {'changes_col': 'artistName', 'api_field': 'artistName'},
    'feed_url': {'changes_col': 'feedUrl', 'api_field': 'feedUrl'}
}

# Columns to check for changes (source table column names)
COLUMNS_TO_CHECK = ['apple_podcast_url', 'category', 'image', 'name', 'artistName', 'feed_url']

# ==========================================
# LOGGING SETUP
# ==========================================

log_dir = "podcast_logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, f"podcast_processor_{datetime.now().strftime('%Y%m%d')}.log")

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

# Directory + file for change details (user requested)
CHANGES_LOG_DIR = "apple_live_updates"
if not os.path.exists(CHANGES_LOG_DIR):
    os.makedirs(CHANGES_LOG_DIR, exist_ok=True)

def get_changes_log_path() -> str:
    """Return path for today's changes logfile inside CHANGES_LOG_DIR"""
    return os.path.join(CHANGES_LOG_DIR, f"changes_{datetime.now().strftime('%Y%m%d')}.log")

def write_changes_log(apple_id: int, changes_details: Dict[str, Dict[str, Any]]) -> None:
    """
    Append human-readable and JSON-formatted change record to the changes log file.
    This function intentionally does not print to console.
    """
    path = get_changes_log_path()
    record = {
        "timestamp": datetime.now().isoformat(),
        "appleId": apple_id,
        "changes": changes_details
    }
    # Compose human-readable header + JSON body
    human_lines = [
        "=" * 80,
        f"CHANGES DETECTED for Apple ID {apple_id} at {record['timestamp']}",
        "-" * 80
    ]
    for col, diff in changes_details.items():
        old_display = diff.get('old') if diff.get('old') else '(None)'
        new_display = diff.get('new') if diff.get('new') else '(None)'
        human_lines.append(f"{col}:")
        human_lines.append(f"  OLD: {old_display}")
        human_lines.append(f"  NEW: {new_display}")
        human_lines.append("-" * 40)
    human_lines.append("JSON:")
    human_lines.append(json.dumps(record, ensure_ascii=False))
    human_lines.append("=" * 80)
    human_text = "\n".join(human_lines) + "\n"

    # Append to file (atomic enough for single-process runs)
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(human_text)
    except Exception as e:
        # If file write fails, log a single-line error (this will go to console and podcast_logs)
        logger.error(f"Failed to write changes to {path} for Apple ID {apple_id}: {e}")

# GLOBAL VARIABLES
shutdown_requested = False

# SIGNAL HANDLING
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.warning(f"Received shutdown signal ({signum}). Will stop after current batch.")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# DATABASE FUNCTIONS
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

def get_last_checkpoint(cursor) -> int:
    """Retrieve the last processed apple_id from checkpoint table"""
    try:
        query = f"""
        SELECT appleId, updateTime 
        FROM `{CHECKPOINT_TABLE}` 
        ORDER BY updateTime DESC 
        LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            last_apple_id, last_run = result
            logger.info(f"Found checkpoint: Last Apple ID = {last_apple_id}, Last Run = {last_run}")
            try:
                return int(last_apple_id) if last_apple_id else 0
            except (ValueError, TypeError):
                logger.warning("Checkpoint appleId is not an integer-like value; returning 0")
                return 0
        else:
            logger.info("No checkpoint found, starting from beginning")
            return 0
    except Error as e:
        logger.error(f"Error fetching checkpoint: {e}")
        return 0


def save_checkpoint(cursor, connection, apple_id: int) -> bool:
    try:
        # Remove any existing checkpoint rows
        delete_query = f"DELETE FROM `{CHECKPOINT_TABLE}`"
        cursor.execute(delete_query)
        
        # Insert the new checkpoint
        insert_query = f"INSERT INTO `{CHECKPOINT_TABLE}` (appleId) VALUES (%s)"
        cursor.execute(insert_query, (str(apple_id),))
        
        connection.commit()
        logger.info(f"Checkpoint replaced with Apple ID = {apple_id}")
        return True
    except Error as e:
        # Attempt rollback if something went wrong
        try:
            connection.rollback()
        except Exception:
            pass
        logger.error(f"Failed to save checkpoint: {e}")
        return False

def fetch_batch_records(cursor, last_apple_id: int, batch_size: int, target_date: str) -> List[Dict]:
    """
    Fetch a batch of records from the source table using apple_id as the record id.
    - Requires 'apple_id' to exist in the source table.
    - Respects the DATE(updated_at) > target_date filter.
    """
    try:
        desired_cols = COLUMNS_TO_CHECK + ['apple_id']

        # Verify which of these columns actually exist in the source table
        info_query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """
        cursor.execute(info_query, (DB_CONFIG['database'], SOURCE_TABLE))
        existing_cols = {row[0] for row in cursor.fetchall()}

        # Keep only columns that exist
        selected_cols = [col for col in desired_cols if col in existing_cols]

        if 'apple_id' not in selected_cols:
            logger.error("'apple_id' column not found in source table. Cannot proceed.")
            return []

        columns_sql = ', '.join(selected_cols)

        query = f"""
        SELECT {columns_sql}
        FROM `{SOURCE_TABLE}`
        WHERE apple_id > %s
          AND DATE(updated_at) > %s
          AND apple_id IS NOT NULL
        ORDER BY apple_id ASC
        LIMIT %s
        """

        cursor.execute(query, (last_apple_id, target_date, batch_size))

        columns_list = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns_list, row)) for row in cursor.fetchall()]

        logger.info(f"Fetched {len(records)} records starting from Apple ID {last_apple_id}")
        return records

    except Error as e:
        logger.error(f"Failed to fetch batch records: {e}")
        return []

# API FUNCTIONS - BATCH PROCESSING
def call_apple_lookup_api_batch(apple_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Call Apple iTunes Lookup API for multiple apple_ids at once.
    Apple API supports comma-separated IDs.
    Returns a dictionary mapping apple_id -> result
    """
    if not apple_ids:
        return {}
    
    try:
        # Join all apple_ids with commas
        ids_param = ','.join(map(str, apple_ids))
        
        params = {
            'id': ids_param,
            'entity': 'podcast'
        }
        
        logger.info(f"Calling API with {len(apple_ids)} apple_ids: {apple_ids}")
        
        response = requests.get(
            API_ENDPOINT,
            params=params,
            timeout=API_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning(f"API returned HTTP {response.status_code}")
            # Return error for all IDs
            return {
                apple_id: {
                    'success': False, 
                    'error': f'HTTP {response.status_code}',
                    'http_code': response.status_code
                } 
                for apple_id in apple_ids
            }
        
        data = response.json()
        
        # Process results
        results_map = {}
        
        if 'results' in data and len(data['results']) > 0:
            # Map each result to its apple_id
            for result in data['results']:
                result_apple_id = result.get('collectionId')
                if result_apple_id:
                    results_map[result_apple_id] = {
                        'success': True,
                        'data': result,
                        'empty': False
                    }
            
            logger.info(f"API returned {len(results_map)} results")
        
        # Mark any IDs that didn't return results as empty
        for apple_id in apple_ids:
            if apple_id not in results_map:
                results_map[apple_id] = {
                    'success': True,
                    'empty': True
                }
                logger.warning(f"No result returned for Apple ID {apple_id}")
        
        return results_map
        
    except requests.exceptions.Timeout:
        logger.error(f"API timeout for batch request")
        return {apple_id: {'success': False, 'error': 'Timeout'} for apple_id in apple_ids}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        return {apple_id: {'success': False, 'error': str(e)} for apple_id in apple_ids}
    
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON response")
        return {apple_id: {'success': False, 'error': 'Invalid JSON'} for apple_id in apple_ids}

# IMAGE URL TRANSFORMATION & NORMALIZATION
def transform_artwork_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    # Remove query params
    url = url.split('?', 1)[0]

    # Find FIRST .jpg or .png
    lower = url.lower()

    jpg_pos = lower.find(".jpg")
    png_pos = lower.find(".png")

    # Determine the earliest valid extension
    positions = [pos for pos in [jpg_pos, png_pos] if pos != -1]

    if not positions:
        # No .jpg or .png found → return the URL untouched
        return url

    end_pos = min(positions) + 4  # include extension ('.jpg' or '.png')

    base = url[:end_pos]

    return base + "/1200x1200bf.webp"


def normalize_image_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    url = url.split('?', 1)[0]
    lower = url.lower()

    jpg_pos = lower.find(".jpg")
    png_pos = lower.find(".png")

    positions = [pos for pos in [jpg_pos, png_pos] if pos != -1]

    if not positions:
        return url

    end_pos = min(positions) + 4

    return url[:end_pos]

def strip_query(url: Optional[str]) -> Optional[str]:
    """Strip query parameters from URL"""
    if not url:
        return None
    
    if '?' in url:
        return url.split('?')[0]
    
    return url

def get_artwork_url(api_data: Dict) -> Optional[str]:
    """Get artwork URL from API response with fallback logic and transform to high-res webp"""
    artwork_fields = ['artworkUrl100', 'artworkUrl60', 'artworkUrl30']
    
    for field in artwork_fields:
        url = api_data.get(field)
        if url:
            return transform_artwork_url(url)
    
    return None

def map_api_to_db_columns(api_data: Dict) -> Dict:
    """Map API response fields to changes table column names and values"""
    mapped_data = {}
    
    for source_col, mapping in COLUMN_MAPPING.items():
        changes_col = mapping['changes_col']
        api_field = mapping['api_field']
        
        if source_col == 'image':
            # Special handling for image field (transformed to high-res webp)
            mapped_data[changes_col] = get_artwork_url(api_data)
        else:
            mapped_data[changes_col] = api_data.get(api_field)
    
    return mapped_data

def detect_changes(old_record: Dict[str, Any], api_data: Dict[str, Any], columns: List[str]) -> Tuple[bool, Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """
    Detect changes between database record and API response.
    Returns: (has_changes: bool, all_values_for_changes_table: Dict, changes_details: Dict)
    all_values contains ALL cleaned API values to save to changes table when ANY change is detected.
    changes_details shows what specifically changed for logging.
    """
    has_changes = False
    all_values = {}
    changes_details = {}

    # Map API response to our changes-table column names
    api_mapped = map_api_to_db_columns(api_data)

    for source_col in columns:
        old_value = old_record.get(source_col)
        changes_col = COLUMN_MAPPING[source_col]['changes_col']
        raw_new_value = api_mapped.get(changes_col)

        # Clean API-side URL fields (strip query params) BEFORE storing/comparing
        if source_col in ['apple_podcast_url', 'feed_url'] and raw_new_value:
            clean_new_value = strip_query(str(raw_new_value))
        else:
            clean_new_value = raw_new_value

        # Save cleaned value into all_values (we'll save ALL values when ANY change detected)
        all_values[changes_col] = clean_new_value

        # Compare normalized versions for change detection
        if source_col == 'image':
            old_norm = normalize_image_url(old_value)
            new_norm = normalize_image_url(clean_new_value)
            if old_norm != new_norm:
                has_changes = True
                changes_details[source_col] = {
                    'old': old_value,
                    'new': clean_new_value
                }
                # do not print; will be written to changes logfile by caller
        else:
            old_str = str(old_value).strip() if old_value is not None else None
            new_str = str(clean_new_value).strip() if clean_new_value is not None else None

            if old_str != new_str:
                has_changes = True
                changes_details[source_col] = {
                    'old': old_value,
                    'new': clean_new_value
                }
                # do not print; will be written to changes logfile by caller

    return has_changes, all_values, changes_details

def save_changes_to_table(cursor, connection, apple_id: int, all_values: Dict) -> bool:
    """
    Save ALL updated values to changes table using UPSERT (INSERT ... ON DUPLICATE KEY UPDATE).
    This will insert new records or update existing ones based on appleId primary key.
    Saves complete data according to changes_table structure.
    """
    try:
        # Build column names and placeholders
        columns = ['appleId'] + list(all_values.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build UPDATE clause for ON DUPLICATE KEY
        update_clause = ', '.join([f"`{col}` = VALUES(`{col}`)" for col in all_values.keys()])
        
        query = f"""
        INSERT INTO `{CHANGES_TABLE}` 
        (`{'`, `'.join(columns)}`) 
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {update_clause},
        updateDate = CURRENT_TIMESTAMP
        """
        
        # Prepare values: appleId + all values
        values = [str(apple_id)] + list(all_values.values())
        
        cursor.execute(query, values)
        connection.commit()
        
        logger.info(f"Saved complete data to changes table for Apple ID {apple_id}: {len(all_values)} columns")
        return True
        
    except Error as e:
        logger.error(f"Failed to save changes for Apple ID {apple_id}: {e}")
        try:
            connection.rollback()
        except Exception:
            pass
        return False

def save_empty_response_to_table(cursor, connection, apple_id: int) -> bool:
    """
    Save/update empty response record to skipped table using UPSERT.
    Increments retryCount each time.
    """
    try:
        query = f"""
        INSERT INTO `{EMPTY_RESPONSE_TABLE}` 
        (appleId, retryCount) 
        VALUES (%s, 1)
        ON DUPLICATE KEY UPDATE
        retryCount = retryCount + 1
        """
       
        cursor.execute(query, (str(apple_id),))
        connection.commit()
        
        # Get the current retry count
        cursor.execute(f"SELECT retryCount FROM `{EMPTY_RESPONSE_TABLE}` WHERE appleId = %s", (str(apple_id),))
        result = cursor.fetchone()
        retry_count = result[0] if result else 1
        
        logger.info(f"Empty response recorded for Apple ID {apple_id} (retry count: {retry_count})")
        return True
        
    except Error as e:
        logger.error(f"Failed to save empty response for Apple ID {apple_id}: {e}")
        return False

# MAIN PROCESSING FUNCTION
def process_batch(connection, target_date: str) -> bool:
    """Main batch processing function - processes exactly BATCH_SIZE records then exits"""
    global shutdown_requested
    
    logger.info("=== STARTING APPLE PODCAST BATCH PROCESSING ===")
    logger.info(f"Target Date: {target_date}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    
    cursor = connection.cursor()
    
    # Get last checkpoint (apple_id)
    last_processed_apple_id = get_last_checkpoint(cursor)
    
    total_changes = 0
    total_empty = 0
    total_errors = 0
    total_no_change = 0
    
    try:
        # Step 1: Fetch records from source table (hold in memory)
        records = fetch_batch_records(cursor, last_processed_apple_id, BATCH_SIZE, target_date)
        
        if not records:
            logger.info("No records to process")
            return True
        
        logger.info(f"Loaded {len(records)} records into memory")
        
        # Step 2: Extract all apple_ids (validate and coerce to int where possible)
        apple_ids = []
        valid_records = []
        for rec in records:
            raw = rec.get('apple_id')
            if raw is None:
                logger.warning("Skipping a row with missing apple_id")
                continue
            try:
                aid = int(raw)
            except (ValueError, TypeError):
                logger.warning(f"Skipping row with non-integer apple_id: {raw}")
                continue
            apple_ids.append(aid)
            valid_records.append(rec)
        
        if not apple_ids:
            logger.info("No valid apple_id values found in fetched rows. Nothing to process.")
            return True
        
        logger.info(f"Apple IDs to process: {apple_ids}")
        
        # Step 3: Call API with all apple_ids at once
        logger.info("Calling Apple API with all IDs...")
        api_results = call_apple_lookup_api_batch(apple_ids)
        
        # Step 4: Process each valid record against API results
        logger.info("Comparing source data with API results...")
        
        last_apple_id_in_batch = None
        
        for idx, record in enumerate(valid_records, 1):
            if shutdown_requested:
                logger.warning("Shutdown requested, stopping")
                break
            
            # Use apple_id as the identifier (no 'id' column assumed)
            raw_apple_id = record.get('apple_id')
            try:
                apple_id = int(raw_apple_id)
            except (ValueError, TypeError):
                logger.warning(f"Skipping record at index {idx} due to invalid apple_id: {raw_apple_id}")
                continue
            
            last_apple_id_in_batch = apple_id
            
            logger.info(f"[{idx}/{len(valid_records)}] Processing (Apple ID: {apple_id})")
            
            # Get API result for this apple_id
            api_result = api_results.get(apple_id)
            
            if not api_result or not api_result.get('success'):
                error_msg = api_result.get('error', 'Unknown') if api_result else 'No API result'
                logger.error(f"API call failed for Apple ID {apple_id}: {error_msg}")
                total_errors += 1
                continue
            
            # Check for empty response
            if api_result.get('empty', False):
                save_empty_response_to_table(cursor, connection, apple_id)
                total_empty += 1
                continue
            
            # Detect changes - now returns all_values and changes_details
            has_changes, all_values, changes_details = detect_changes(record, api_result['data'], COLUMNS_TO_CHECK)
            
            if has_changes:
                # Write changes to the dedicated changes logfile (do NOT print to console)
                write_changes_log(apple_id, changes_details)
                
                # Save ALL values (complete data according to changes_table structure)
                if save_changes_to_table(cursor, connection, apple_id, all_values):
                    total_changes += 1
                else:
                    total_errors += 1
            else:
                logger.info(f"No changes detected for Apple ID {apple_id}")
                total_no_change += 1
        
        # Step 5: Save checkpoint with the last apple_id processed
        if last_apple_id_in_batch:
            save_checkpoint(cursor, connection, last_apple_id_in_batch)
        
        logger.info("=== BATCH PROCESSING COMPLETE ===")
        logger.info(f"Total Processed: {len(valid_records)}")
        logger.info(f"Changes Detected: {total_changes}")
        logger.info(f"No Changes: {total_no_change}")
        logger.info(f"Empty Responses: {total_empty}")
        logger.info(f"Errors: {total_errors}")
        logger.info(f"Last Processed Apple ID: {last_apple_id_in_batch}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during batch processing: {e}", exc_info=True)
        return False
    finally:
        cursor.close()

# MAIN ENTRY POINT
def main():
    """Main entry point"""
    
    logger.info("=== APPLE PODCAST BATCH PROCESSOR STARTED ===")
    
    # Get target date from command line or use today
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    # Connect to database
    connection = get_db_connection()
    
    try:
        success = process_batch(connection, target_date)
        if success:
            logger.info("Script completed successfully")
        else:
            logger.error("Script completed with errors")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if connection.is_connected():
            connection.close()
            logger.info("Database connection closed")
    
    logger.info("=== SCRIPT FINISHED ===")

if __name__ == "__main__":
    main()
