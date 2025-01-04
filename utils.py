import pandas as pd
import numpy as np
from io import BytesIO
import zipfile
import re

def clean_nan_values(df):
    """Replace NaN values with empty strings in the DataFrame"""
    return df.replace({np.nan: '', 'nan': '', 'NaN': ''})

def clean_number_to_text(df):
    """Converts all numeric columns to string with integer formatting"""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].apply(lambda x: f"{int(x)}" if pd.notnull(x) else "")
    return df

def create_zip_file(dfs_dict):
    """Create a zip file containing all CSV files"""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, df in dfs_dict.items():
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            if filename.endswith('.csv'):
                zip_file.writestr(filename, csv_buffer.getvalue())
            else:
                zip_file.writestr(f"{filename}.csv", csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def clean_number(phone):
    """Clean and standardize phone numbers consistently"""
    phone = str(phone)
    phone = re.sub(r'\D', '', phone)
    if phone.startswith('1') and len(phone) > 10:
        phone = phone[1:]
    return phone