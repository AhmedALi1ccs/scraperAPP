import pandas as pd
from collections import defaultdict
from utils import clean_nan_values, clean_number_to_text, clean_number

def process_files(log_dfs, list_df, conditions, log_filenames):
    """
    Process log files by removing specific phone numbers based on conditions.
    Returns separate removed records for each log file with only the removed numbers.
    
    Args:
        log_dfs (list): List of DataFrames containing log data
        list_df (DataFrame): DataFrame containing phone numbers and log types
        conditions (list): List of dictionaries with type and threshold conditions
        log_filenames (list): List of log file names
        
    Returns:
        tuple: (cleaned list_df, list of updated log_dfs, list of removed records)
    """
    # Step 1: Efficiently count occurrences using defaultdict
    occurrence_counter = defaultdict(int)
    list_records = list_df.to_dict('records')
    
    for record in list_records:
        key = (record['Log Type'].title(), clean_number(str(record['Phone'])))
        occurrence_counter[key] += 1
    
    # Step 2: Identify phones to remove based on conditions
    phones_to_remove = set()
    for cond in conditions:
        for (log_type, phone), count in occurrence_counter.items():
            if log_type == cond['type'] and count >= cond['threshold']:
                phones_to_remove.add(phone)
    
    # Step 3: Process list DataFrame efficiently
    kept_records = []
    removed_records = []
    
    for record in list_records:
        cleaned_phone = clean_number(str(record['Phone']))
        if cleaned_phone in phones_to_remove:
            removed_records.append(record)
        else:
            kept_records.append(record)
    
    list_df_kept = pd.DataFrame(kept_records) if kept_records else pd.DataFrame(columns=list_df.columns)
    removed_from_list = pd.DataFrame(removed_records) if removed_records else pd.DataFrame(columns=list_df.columns)
    
    # Step 4: Process log files with optimized phone column detection
    def get_phone_columns(df):
        """Helper function to identify phone number columns."""
        phone_keywords = {'mobile', 'phone', 'number', 'tel', 'contact', 'ph', 'landline', 'voip'}
        return {
            col.strip().lower(): col for col in df.columns 
            if any(keyword in col.strip().lower() for keyword in phone_keywords)
        }
    
    updated_log_dfs = []
    removed_log_records = []
    
    for log_df, filename in zip(log_dfs, log_filenames):
        # Convert to records for memory efficiency
        log_records = log_df.to_dict('records')
        phone_columns = get_phone_columns(log_df)
        
        if not phone_columns:
            print(f"No phone columns found in {filename}")
            updated_log_dfs.append(log_df.copy())
            removed_log_records.append(pd.DataFrame(columns=log_df.columns))
            continue
        
        processed_records = []
        removed_log = []
        
        for record in log_records:
            record_removed = False
            removed_record = record.copy()
            
            for col_lower, original_col in phone_columns.items():
                phone = str(record[original_col])
                
                try:
                    # Apply thorough cleaning similar to Version 2
                    if phone.replace(".", "").isdigit():
                        phone = f"{int(float(phone))}"
                    cleaned_phone = clean_number(phone)
                    
                    if cleaned_phone in phones_to_remove:
                        record_removed = True
                        record[original_col] = ''
                        # Clear other phone columns in removed record
                        for other_col in phone_columns.values():
                            if other_col != original_col:
                                removed_record[other_col] = ''
                except (ValueError, TypeError):
                    continue
            
            if record_removed:
                removed_log.append(removed_record)
            processed_records.append(record)
        
        # Convert back to DataFrames and clean
        processed_df = pd.DataFrame(processed_records)
        removed_df = pd.DataFrame(removed_log) if removed_log else pd.DataFrame(columns=log_df.columns)
        
        processed_df = clean_nan_values(processed_df)
        removed_df = clean_nan_values(removed_df)
        
        updated_log_dfs.append(processed_df)
        removed_log_records.append(removed_df)
    
    # Final cleanup
    list_df_kept = clean_nan_values(list_df_kept)
    removed_from_list = clean_nan_values(removed_from_list)
    
    return list_df_kept, updated_log_dfs, removed_log_records
