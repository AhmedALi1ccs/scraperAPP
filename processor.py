import pandas as pd
from utils import clean_nan_values, clean_number_to_text, clean_number
from collections import Counter

def process_files(log_dfs, list_df, conditions, log_filenames):
    """
    Process log files by removing specific phone numbers based on conditions.
    Returns separate removed records for each log file with only the removed numbers.
    """
    # Convert list_df to simple dict structure
    phone_data = list_df.to_dict('records')
    
    # Count occurrences using Counter
    occurrence_counter = Counter(
        (row['Log Type'], clean_number(str(row['Phone']))) 
        for row in phone_data
    )
    
    # Find phones to remove
    phones_to_remove = set()
    for cond in conditions:
        for (log_type, phone), count in occurrence_counter.items():
            if log_type.title() == cond['type'] and count >= cond['threshold']:
                phones_to_remove.add(phone)
    
    # Split records into kept and removed
    kept_records = []
    removed_records = []
    
    for row in phone_data:
        cleaned_phone = clean_number(str(row['Phone']))
        if cleaned_phone in phones_to_remove:
            removed_records.append(row)
        else:
            kept_records.append(row)
            
    # Convert back to DataFrames
    list_df_kept = pd.DataFrame(kept_records) if kept_records else pd.DataFrame(columns=list_df.columns)
    removed_from_list = pd.DataFrame(removed_records) if removed_records else pd.DataFrame(columns=list_df.columns)
    
    # Process log files
    updated_log_dfs = []
    removed_log_records = []
    
    for log_df, filename in zip(log_dfs, log_filenames):
        # Convert to records for easier processing
        log_records = log_df.to_dict('records')
        processed_records = []
        removed_log = []
        
        # Normalize column names
        columns = {col.strip().lower(): col for col in log_df.columns}
        
        # Find phone columns
        phone_columns = [
            col for col in columns.keys()
            if any(phrase in col for phrase in 
                  ['mobile', 'phone', 'number', 'tel', 'contact', 'ph', 'landline', 'voip'])
        ]
        
        if not phone_columns:
            print(f"No phone columns found in {filename}")
            updated_log_dfs.append(log_df.copy())
            removed_log_records.append(pd.DataFrame(columns=log_df.columns))
            continue
            
        # Process each record
        for record in log_records:
            record_removed = False
            removed_record = record.copy()
            
            for col_lower in phone_columns:
                col = columns[col_lower]
                phone = str(record[col])
                
                try:
                    cleaned_phone = clean_number(phone)
                    if cleaned_phone in phones_to_remove:
                        record_removed = True
                        record[col] = ''
                        # Keep only the removed number in removed record
                        for other_col_lower in phone_columns:
                            other_col = columns[other_col_lower]
                            if other_col != col:
                                removed_record[other_col] = ''
                except:
                    continue
                    
            if record_removed:
                removed_log.append(removed_record)
            processed_records.append(record)
            
        # Convert processed records back to DataFrame
        processed_df = pd.DataFrame(processed_records)
        removed_df = pd.DataFrame(removed_log) if removed_log else pd.DataFrame(columns=log_df.columns)
        
        # Clean up NaN values
        processed_df = clean_nan_values(processed_df)
        removed_df = clean_nan_values(removed_df)
        
        updated_log_dfs.append(processed_df)
        removed_log_records.append(removed_df)
    
    # Clean up final DataFrames
    list_df_kept = clean_nan_values(list_df_kept)
    removed_from_list = clean_nan_values(removed_from_list)
    
    return list_df_kept, updated_log_dfs, removed_log_records
