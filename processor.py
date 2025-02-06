import pandas as pd
from collections import defaultdict
from utils import clean_nan_values, clean_number_to_text, clean_number
def normalize_phone(value):
    """Convert any phone number format to a consistent string format."""
    if pd.isna(value) or value == '' or value is None:
        return ''
        
    # Convert to string and clean
    try:
        # Handle float/integer
        if isinstance(value, (float, int)):
            value = str(int(value))  # Remove decimal point if present
        else:
            value = str(value).strip()
            
        # Remove any non-digit characters
        value = ''.join(filter(str.isdigit, value))
        return value if value else ''
    except:
        return ''

def is_valid_phone(value):
    """Validate if a phone number is valid."""
    if pd.isna(value) or value == '' or value is None:
        return False
        
    phone = normalize_phone(value)
    return len(phone) >= 7 if phone else False

def get_phone_columns(df):
    """Identify all phone-related columns in the DataFrame."""
    phone_keywords = {
        'mobile', 'phone', 'number', 'tel', 'contact', 
        'ph', 'landline', 'voip', 'cell'
    }
    return [
        col for col in df.columns 
        if any(keyword in col.lower() for keyword in phone_keywords)
    ]

def convert_phone_columns_to_string(df):
    """Convert all phone number columns to string format."""
    phone_cols = get_phone_columns(df)
    for col in phone_cols:
        df[col] = df[col].apply(normalize_phone)
    return df

def process_files(log_dfs, list_df, conditions, log_filenames):
    """Process files with consistent phone number handling."""
    # Initial cleanup and type conversion
    cleaned_list_df = clean_nan_values(list_df)
    cleaned_list_df = convert_phone_columns_to_string(cleaned_list_df)
    
    cleaned_log_dfs = []
    for df in log_dfs:
        cleaned_df = clean_nan_values(df)
        cleaned_df = convert_phone_columns_to_string(cleaned_df)
        cleaned_log_dfs.append(cleaned_df)
    
    # Step 1: Count occurrences
    occurrence_counter = defaultdict(int)
    for _, row in cleaned_list_df.iterrows():
        phone = normalize_phone(row['Phone'])
        if is_valid_phone(phone):
            key = (str(row['Log Type']).title(), phone)
            occurrence_counter[key] += 1
    
    # Step 2: Identify phones to remove
    phones_to_remove = {}
    for (log_type, phone), count in occurrence_counter.items():
        for cond in conditions:
            if log_type == cond['type'] and count >= cond['threshold']:
                phones_to_remove[phone] = (log_type, count)
    
    # Step 3: Process list DataFrame
    list_df_scrubbed = cleaned_list_df.copy()
    removed_from_list = cleaned_list_df.copy()
    
    removal_mask = cleaned_list_df['Phone'].apply(
        lambda x: is_valid_phone(x) and normalize_phone(x) in phones_to_remove
    )
    
    # Add removal reasons and prepare removed records
    for idx in cleaned_list_df[removal_mask].index:
        phone = normalize_phone(cleaned_list_df.at[idx, 'Phone'])
        if phone in phones_to_remove:
            log_type, count = phones_to_remove[phone]
            removed_from_list.at[idx, 'Removal_Reason'] = f"Removed due to {log_type} count: {count}"
            list_df_scrubbed.at[idx, 'Phone'] = ''
    
    removed_from_list = removed_from_list[removal_mask].copy()
    
    # Step 4: Process log files
    updated_log_dfs = []
    removed_log_records = []
    
    for log_df, filename in zip(cleaned_log_dfs, log_filenames):
        scrubbed_df = log_df.copy()
        removed_df = log_df.copy()
        
        phone_cols = get_phone_columns(log_df)
        
        if not phone_cols:
            print(f"No phone columns found in {filename}")
            updated_log_dfs.append(scrubbed_df)
            removed_log_records.append(pd.DataFrame(columns=log_df.columns))
            continue
        
        records_with_triggers = set()
        trigger_info = {}
        
        # First pass - identify records with triggering numbers
        for idx in log_df.index:
            for col in phone_cols:
                phone = normalize_phone(log_df.at[idx, col])
                if is_valid_phone(phone) and phone in phones_to_remove:
                    records_with_triggers.add(idx)
                    log_type, count = phones_to_remove[phone]
                    if idx not in trigger_info:
                        trigger_info[idx] = []
                    trigger_info[idx].append((col, phone, log_type, count))
                    scrubbed_df.at[idx, col] = ''
        
        # Second pass - prepare removed records
        for idx in records_with_triggers:
            # Blank out ALL phone numbers first
            for col in phone_cols:
                removed_df.at[idx, col] = ''
            
            # Then put back ONLY the triggering numbers
            reasons = []
            for col, phone, log_type, count in trigger_info[idx]:
                removed_df.at[idx, col] = phone
                reasons.append(f"Number {phone} in column '{col}' exceeded {log_type} count: {count}")
            
            removed_df.at[idx, 'Removal_Reason'] = ' | '.join(reasons)
            removed_df.at[idx, 'Removal_Date'] = pd.Timestamp.now().strftime('%d/%m/%Y')
        
        # Keep only records with triggers in removed file
        removed_df = removed_df[removed_df.index.isin(records_with_triggers)].copy()
        
        updated_log_dfs.append(scrubbed_df)
        removed_log_records.append(removed_df)
    
    return list_df_scrubbed, updated_log_dfs, removed_log_records
