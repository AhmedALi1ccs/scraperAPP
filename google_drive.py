import os
import json
from io import BytesIO
import csv
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from dotenv import load_dotenv
from utils import format_dataframe_for_export

class GoogleDriveManager:
    SCOPES = ['https://www.googleapis.com/auth/drive']
    REMOVED_FOLDER_ID = "18evx04gWua9ls1mDiIr5FvAQhdFbrwfr"
    SCRUBBED_FOLDER_ID = "1-jYrCY5ev44Hy5fXVwOZSjw7xPSTy9ML"

    def __init__(self):
        load_dotenv()
        self.service = self._authenticate()

    def _authenticate(self):
        """Authenticate with Google Drive API"""
        try:
            credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
            if not credentials_json:
                raise ValueError("GOOGLE_CREDENTIALS_JSON not found in environment variables.")
            
            credentials_dict = json.loads(credentials_json)
            creds = Credentials.from_service_account_info(credentials_dict, scopes=self.SCOPES)
            return build('drive', 'v3', credentials=creds)
        except Exception as e:
            print(f"Google Drive authentication failed: {e}")
            raise e

    def upload_dataframe(self, df, filename, folder_id):
        """Upload a DataFrame as a properly formatted CSV file to Google Drive"""
        try:
            # Format DataFrame
            formatted_df = format_dataframe_for_export(df)
            
            # Create CSV in memory with proper formatting
            csv_buffer = BytesIO()
            formatted_df.to_csv(
                csv_buffer,
                index=False,
                encoding='utf-8-sig',
                lineterminator='\n',
                quoting=csv.QUOTE_MINIMAL,
                sep=',',
                float_format='%.2f'
            )
            csv_buffer.seek(0)

            # Prepare file metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id],
                'mimeType': 'text/csv'
            }

            # Create media upload object
            media = MediaIoBaseUpload(
                csv_buffer,
                mimetype='text/csv',
                resumable=True
            )

            # Execute upload
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            print(f"Uploaded {filename} to Google Drive successfully! File ID: {file.get('id')}")
            return file.get('id')
        
        except Exception as e:
            print(f"Failed to upload {filename}: {e}")
            raise e
