import pandas as pd
from utils import clean_nan_values, clean_number_to_text, clean_number

def process_files(log_dfs, list_df, conditions, log_filenames):
    """
    Process log files by removing specific phone numbers based on conditions.
    Returns separate removed records for each log file with only the removed numbers.
    """
    # Normalize list file phone numbers
    list_df["Phone"] = list_df["Phone"].astype(str).apply(clean_number)

    # Compute occurrences in the list file
    list_occurrences = (
        list_df.groupby(["Log Type", "Phone"])
        .size()
        .reset_index(name="occurrence")
    )
    list_df = pd.merge(list_df, list_occurrences, on=["Log Type", "Phone"], how="left")

    # Initialize containers
    removed_from_list = pd.DataFrame()
    updated_log_dfs = []
    removed_log_records = []

    # Parse conditions and identify numbers to remove
    cleaned_phones_to_remove = []
    for cond in conditions:
        matching_numbers = list_df.loc[
            (list_df["Log Type"].str.title() == cond["type"]) &
            (list_df["occurrence"] >= cond["threshold"]), 
            "Phone"
        ].unique()

        if len(matching_numbers) > 0:
            current_removed = list_df[list_df["Phone"].isin(matching_numbers)]
            removed_from_list = pd.concat([removed_from_list, current_removed])
            list_df = list_df[~list_df["Phone"].isin(matching_numbers)]
            cleaned_phones_to_remove.extend([clean_number(phone) for phone in matching_numbers])

    # Remove duplicates from cleaned_phones_to_remove
    cleaned_phones_to_remove = list(set(cleaned_phones_to_remove))

    # Process each log file
    for log_df, filename in zip(log_dfs, log_filenames):
        processed_log_df = log_df.copy()
        removed_records = []

        # Normalize column names
        processed_log_df.columns = processed_log_df.columns.str.strip().str.lower()
        processed_log_df = processed_log_df.astype(str)
        processed_log_df = clean_number_to_text(processed_log_df)

        # Identify potential phone number columns
        phone_columns = [
            col for col in processed_log_df.columns
            if any(phrase in col.lower() for phrase in 
                  ['mobile', 'phone', 'number', 'tel', 'contact', 'ph', 'landline','voip'])
        ]

        if not phone_columns:
            print(f"No phone columns found in {filename}")
            updated_log_dfs.append(processed_log_df)
            removed_log_records.append(pd.DataFrame())
            continue

        # Track rows that had numbers removed along with the removed numbers
        for col in phone_columns:
            # Clean the column's phone numbers
            processed_log_df[col] = processed_log_df[col].astype(str).apply(
                lambda x: f"{int(float(x))}" if x.replace(".", "").isdigit() else x
            )
            processed_log_df = clean_number_to_text(processed_log_df)
            original_values = processed_log_df[col].copy()
            cleaned_column = processed_log_df[col].apply(clean_number)
            
            # Identify rows to remove
            remove_mask = cleaned_column.isin(cleaned_phones_to_remove)
            
            if remove_mask.any():
                # For each row where a number was removed, store the row with only the removed number
                for idx in processed_log_df[remove_mask].index:
                    removed_row = processed_log_df.loc[idx].copy()
                    original_number = original_values[idx]
                    
                    # Clear all phone columns in the removed row
                    for phone_col in phone_columns:
                        removed_row[phone_col] = ''
                    
                    # Put back only the removed number in the current column
                    removed_row[col] = original_number
                    removed_records.append(removed_row)
                
                # Replace matching numbers with empty string in processed DataFrame
                processed_log_df.loc[remove_mask, col] = ''

        # Create removed records DataFrame for this log file
        if removed_records:
            removed_records_df = pd.DataFrame(removed_records)
            removed_records_df = clean_nan_values(removed_records_df)
        else:
            removed_records_df = pd.DataFrame()

        # Clean NaN values from processed log DataFrame
        processed_log_df = clean_nan_values(processed_log_df)

        # Add the processed DataFrames to their respective lists
        updated_log_dfs.append(processed_log_df)
        removed_log_records.append(removed_records_df)

    # Clean NaN values from list DataFrames
    list_df = clean_nan_values(list_df)
    removed_from_list = clean_nan_values(removed_from_list)

    return list_df, updated_log_dfs, removed_log_records