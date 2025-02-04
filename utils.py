import pandas as pd
import numpy as np
from io import BytesIO
import zipfile
import re
import csv

def clean_nan_values(df):
    """Replace NaN values and its string variants with empty strings in the DataFrame"""
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Replace various forms of NaN with empty string
    replacements = {
        np.nan: '',
        'nan': '',
        'NaN': '',
        'Nan': '',
        'NA': '',
        'None': '',
        'null': '',
        'NULL': '',
        'NAN': ''
    }
    
    # Apply replacements
    df = df.replace(replacements)
    
    # Additional cleaning for any remaining NaN-like strings
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].apply(
                lambda x: '' if pd.isna(x) or str(x).lower() in ['nan', 'none', 'null'] else x
            )
    
    return df

def clean_number_to_text(df):
    """Converts all numeric columns to string with integer formatting"""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].apply(lambda x: f"{int(x)}" if pd.notnull(x) else "")
    return df

def clean_number(phone):
    """Clean and standardize phone numbers consistently"""
    phone = str(phone)
    phone = re.sub(r'\D', '', phone)
    if phone.startswith('1') and len(phone) > 10:
        phone = phone[1:]
    return phone

def format_dataframe_for_export(df):
    """Prepare DataFrame for export with proper formatting"""
    formatted_df = df.copy()
    
    # Process each column
    for column in formatted_df.columns:
        # Get column data type
        col_type = formatted_df[column].dtype
        
        if col_type == 'object':
            # Replace NaN with empty string first
            formatted_df[column] = formatted_df[column].fillna('')
            # Clean string values
            formatted_df[column] = formatted_df[column].astype(str)
            formatted_df[column] = formatted_df[column].apply(lambda x: '' if x.lower() in ['nan', 'none', 'null'] else x.strip())
            formatted_df[column] = formatted_df[column].replace({'\n': ' ', '\r': ' '})
            
        elif np.issubdtype(col_type, np.number):
            # Format numbers consistently and handle NaN
            formatted_df[column] = formatted_df[column].apply(
                lambda x: f"{int(x)}" if pd.notnull(x) and float(x).is_integer() 
                else (f"{x:.2f}" if pd.notnull(x) else '')
            )
            
        # Handle date columns if any
        elif pd.api.types.is_datetime64_any_dtype(col_type):
            formatted_df[column] = formatted_df[column].fillna('')
            formatted_df[column] = formatted_df[column].apply(
                lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else '')
    
    # Clean column names
    formatted_df.columns = formatted_df.columns.str.strip()
    formatted_df.columns = formatted_df.columns.str.replace('[^\w\s-]', '')
    
    return formatted_df

def create_zip_file(dfs_dict):
    """Create a zip file containing properly formatted CSV files"""
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, df in dfs_dict.items():
            # Create a buffer for the CSV
            csv_buffer = BytesIO()
            
            # Format DataFrame before saving
            formatted_df = format_dataframe_for_export(df)
            
            # Save with explicit formatting
            formatted_df.to_csv(
                csv_buffer,
                index=False,
                encoding='utf-8-sig',  # Adds BOM for Excel compatibility
                lineterminator='\n',   # Unix-style line endings
                quoting=csv.QUOTE_MINIMAL,  # Quote only necessary fields
                sep=',',  # Explicit separator
                float_format='%.2f'  # Format floating point numbers
            )
            
            csv_buffer.seek(0)
            
            # Save to zip with proper filename
            final_filename = f"{filename}.csv" if not filename.endswith('.csv') else filename
            zip_file.writestr(final_filename, csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def prepare_dataframe_for_export(df):
    """Prepare DataFrame for CSV export by cleaning and validating data"""
    # Make a copy to avoid modifying original
    df = df.copy()
    
    # Clean column names
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace('\n', ' ')
    df.columns = df.columns.str.replace('\r', ' ')
    
    # Handle special characters and encoding issues
    for column in df.columns:
        if df[column].dtype == 'object':
            # Replace problematic characters
            df[column] = df[column].astype(str).apply(lambda x: x.strip())
            df[column] = df[column].replace({'\n': ' ', '\r': ' '})
            
        # Convert numeric columns to proper format
        elif df[column].dtype in ['int64', 'float64']:
            # Handle NaN values
            df[column] = df[column].fillna('')
            # Convert to string with proper formatting
            df[column] = df[column].apply(
                lambda x: f"{int(x)}" if x != '' and not pd.isna(x) else ''
            )
    
    return df
