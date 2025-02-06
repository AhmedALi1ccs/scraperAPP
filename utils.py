import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import zipfile
import re
import csv
import os
import datetime
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


def clean_filename(filename):
    """Sanitize filenames to remove invalid characters."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename).strip()

import csv
from io import BytesIO
import zipfile
from datetime import datetime
def format_value(val):
    """Format value to match the exact format shown in the example."""
    if pd.isna(val) or val == '':
        return ''
    
    # Convert to string and clean
    val_str = str(val).strip()
    
    # Handle date format
    if isinstance(val, pd.Timestamp):
        return val.strftime('%d/%m/%Y')
    
    # Check if it's a date string
    if isinstance(val_str, str) and len(val_str) == 10 and val_str[4] == '-' and val_str[7] == '-':
        try:
            date = pd.to_datetime(val_str)
            return date.strftime('%d/%m/%Y')
        except:
            pass
    
    # Handle numeric values
    try:
        if ',' in val_str:  # If value contains comma, need to quote it
            return f'"{val_str}"'
        if float(val_str) == int(float(val_str)):
            return str(int(float(val_str)))
        return str(float(val_str))
    except ValueError:
        # If string contains comma, quote it
        if ',' in val_str:
            return f'"{val_str}"'
        return val_str

def create_zip_file(dfs_dict):
    """Create a zip file containing CSV files with proper field alignment."""
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for filename, df in dfs_dict.items():
            # Process DataFrame
            processed_df = df.copy()
            
            # Ensure all columns exist
            for col in processed_df.columns:
                if col not in processed_df.columns:
                    processed_df[col] = ''
            
            # Get expected number of columns
            expected_columns = len(processed_df.columns)
            
            # Convert all columns to strings with proper formatting
            for col in processed_df.columns:
                processed_df[col] = processed_df[col].apply(format_value)
            
            # Create buffer
            csv_buffer = StringIO()
            
            # Write headers
            header_row = ','.join(str(col) for col in processed_df.columns)
            csv_buffer.write(header_row + '\n')
            
            # Write data rows with alignment check
            for idx, row in processed_df.iterrows():
                # Format row values
                row_values = [str(val) for val in row]
                
                # Ensure row has correct number of fields
                if len(row_values) < expected_columns:
                    row_values.extend([''] * (expected_columns - len(row_values)))
                elif len(row_values) > expected_columns:
                    row_values = row_values[:expected_columns]
                
                # Write row
                row_str = ','.join(row_values)
                csv_buffer.write(row_str + '\n')
            
            # Clean filename
            safe_filename = filename.replace('/', '_').replace('\\', '_')
            if not safe_filename.lower().endswith('.csv'):
                safe_filename += '.csv'
            
            # Write to zip
            csv_buffer.seek(0)
            content = csv_buffer.getvalue()
            
            # Double-check content before writing
            lines = content.split('\n')
            header_count = len(header_row.split(','))
            
            # Verify each line has correct number of fields
            verified_lines = []
            for line in lines:
                if line:  # Skip empty lines
                    fields = line.split(',')
                    if len(fields) < header_count:
                        fields.extend([''] * (header_count - len(fields)))
                    elif len(fields) > header_count:
                        fields = fields[:header_count]
                    verified_lines.append(','.join(fields))
            
            # Write verified content
            verified_content = '\n'.join(verified_lines)
            zip_file.writestr(safe_filename, verified_content.encode('utf-8'))
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def prepare_dataframe_for_export(df):
    """Prepare DataFrame ensuring exact format matching and field alignment."""
    df = df.copy()
    
    # Process each column
    for col in df.columns:
        df[col] = df[col].apply(format_value)
    
    return df
