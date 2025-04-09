#!/usr/bin/env python3
"""
Utility functions for uploading files to Google Drive.

This module provides functions to upload reports to Google Drive
and makes them accessible to team members.
"""

import logging
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from .config import LOGGING_CONFIG, RETURN_DATA_FILE, DAILY_COUNT_FILE

# Configure logging
logging.basicConfig(
    level=logging.getLevelName(LOGGING_CONFIG["level"]), 
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def upload_to_gdrive(file_path, folder_id=None):
    """
    Upload a file to Google Drive using a service account.
    
    Args:
        file_path (str): Path to the file to upload
        folder_id (str, optional): Google Drive folder ID to upload to
        
    Returns:
        str: URL of the uploaded file or None on failure
    """
    try:
        # Authenticate using the service account
        SCOPES = ['https://www.googleapis.com/auth/drive']
        SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to service account key file
        
        # Check if service account file exists
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logger.error(f"Service account file {SERVICE_ACCOUNT_FILE} not found")
            return None
            
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        
        # Get the base filename without the path
        filename = os.path.basename(file_path)
        
        # Search for the file by name in the specified folder
        query = f"name='{filename}'"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        
        results = service.files().list(q=query, spaces='drive').execute()
        items = results.get('files', [])
        
        if items:
            # Update the existing file
            file_id = items[0]['id']
            media = MediaFileUpload(file_path, resumable=True)
            updated_file = service.files().update(fileId=file_id, media_body=media).execute()
            file_url = f"https://drive.google.com/file/d/{updated_file['id']}/view"
            logger.info(f"Updated {filename} in Google Drive: {file_url}")
        else:
            # Upload as a new file
            file_metadata = {'name': filename}
            if folder_id:
                file_metadata['parents'] = [folder_id]
                
            media = MediaFileUpload(file_path, resumable=True)
            new_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            file_id = new_file['id']
            file_url = f"https://drive.google.com/file/d/{file_id}/view"
            logger.info(f"Uploaded {filename} to Google Drive: {file_url}")
        
        # Make the file public with writer access
        permission = {
            'type': 'anyone',
            'role': 'writer',
        }
        service.permissions().create(fileId=file_id, body=permission).execute()
        logger.info(f"Made {filename} public with writer access")
        
        return file_url
        
    except Exception as e:
        logger.error(f"Error uploading {file_path} to Google Drive: {e}")
        return None

def upload_reports():
    """
    Upload all generated reports to Google Drive.
    
    Uploads both daily profits and bundle return reports.
    """
    logger.info("Uploading reports to Google Drive")
    
    # Upload daily profits report
    if os.path.exists(DAILY_COUNT_FILE):
        url = upload_to_gdrive(DAILY_COUNT_FILE)
        if url:
            logger.info(f"Daily profits report available at: {url}")
    else:
        logger.warning(f"Daily profits report not found at {DAILY_COUNT_FILE}")
    
    # Upload bundle returns report
    if os.path.exists(RETURN_DATA_FILE):
        url = upload_to_gdrive(RETURN_DATA_FILE)
        if url:
            logger.info(f"Bundle returns report available at: {url}")
    else:
        logger.warning(f"Bundle returns report not found at {RETURN_DATA_FILE}")

if __name__ == "__main__":
    # Test the upload functionality
    upload_reports()