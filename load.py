import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import sqlite3

# from google.oauth2 import InstalledAppFlow
# from googleapiclient.discovery import build
# from google.auth.transport.requests import Request
# from google.auth.credentials import Credentials

import gspread
from oauth2client.service_account import ServiceAccountCredentials  # For Google Sheets authentication
import pandas as pd
from gspread_dataframe import set_with_dataframe  # To write DataFrame to Google Sheets
from pydrive.auth import GoogleAuth  # For Google Drive authentication
from pydrive.drive import GoogleDrive  # For Google Drive operations


def load_data(**kwargs):
    # Pull the file path from XCOM
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='fetch_and_save_data')  # Replace with your extract task id
    
    print(f"File path pulled from XCOM: {file_path}")
    
    if file_path:
        # Step 1: Load data from CSV into a DataFrame
        df = pd.read_csv(file_path)

            # Connect to SQLite database
        conn = sqlite3.connect('/home/muneeb/airflow/dags/data.db')
        
        # Upload DataFrame to SQLite
        df.to_sql('consumer_com_table', conn, if_exists='replace', index=False)
        
        # Close connection
        conn.close()


        print("Data has been successfully uploaded to the database")


def upload_data_gsheets(**kwargs):
    # Pull the file path from XCOM
    ti = kwargs['ti']

    file_path = ti.xcom_pull(key='file_path', task_ids='transform_data') 


    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    credentials = ServiceAccountCredentials.from_json_keyfile_name('/home/muneeb/airflow/dags/gsheets_credentials.json', scopes)

    gc = gspread.authorize(credentials)

    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)

    gs = gc.open_by_key("1FHu70GZ0LM6iGvLk2fZkvulxUwgtYz-TV402f7ROHlU")

    worksheet1 = gs.worksheet('Complaints')

    df = pd.read_csv(file_path)
    # worksheet1.clear()
    # set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False, include_column_header=True, resize = True, )

    existing_rows = len(worksheet1.get_all_values())

    next_row = existing_rows + 1

    set_with_dataframe(worksheet=worksheet1, dataframe=df, row=next_row, include_index=False, include_column_header=False, resize=True)
    print("Transformed Data Has Been Successfully Uploaded on Google Sheets!")
