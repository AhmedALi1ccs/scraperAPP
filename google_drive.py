import os
import json
from io import BytesIO
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from dotenv import load_dotenv

class GoogleDriveManager:
    SCOPES = ['https://www.googleapis.com/auth/drive']
    REMOVED_FOLDER_ID = "1NWv0AjsOF-_5lmsEyL1q20liFWn1CtUk"
    SCRUBBED_FOLDER_ID = "1Ink3w5hpU5sAx9EvFmPu33W7HIbE1BIz"

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
        """Upload a DataFrame as a CSV file to Google Drive"""
        try:
            # Convert DataFrame to CSV in memory
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            # Prepare file metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
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