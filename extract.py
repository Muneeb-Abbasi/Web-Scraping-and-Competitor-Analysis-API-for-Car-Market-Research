import requests
import json
from datetime import date, datetime, timedelta
import csv
import logging

def fetch_and_save_data(**kwargs):

    list_of_states=list(requests.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json").json().keys())
    # print(list_of_states)


    size = 500 #500
    time_delta=365 #365
    max_date = (date.today()).strftime("%Y-%m-%d")
    min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")

       # Ensure path is correct
    file_path = '/path/to/your/directory/consumer_complaints.csv'
    logging.info(f"Saving data to {file_path}")

    # API URL with placeholders
    url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'

    # Open a CSV file to write the results
    with open('/home/muneeb/airflow/dags/consumer_complaints.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write the header row with all the specified fields
        writer.writerow([
            'product', 'complaint_what_happened', 'date_sent_to_company', 'issue', 'sub_product', 
            'zip_code', 'tags', 'has_narrative', 'complaint_id', 'timely', 'consumer_consent_provided', 
            'company_response', 'submitted_via', 'company', 'date_received', 'state', 
            'consumer_disputed', 'company_public_response', 'sub_issue'
        ])

        # Loop through each state and fetch data
        for state in list_of_states:
            response = requests.get(url.format(size, max_date, min_date, state))
            
            # Check if the request was successful
            if response.status_code == 200:
                data = response.json()
                
                # Access the complaints data from the 'hits' key
                complaints = data.get('hits', {}).get('hits', [])
                
                # Loop through each complaint and extract the relevant data
                for complaint in complaints:
                    complaint_data = complaint['_source']
                    
                    # Extract fields from the complaint data
                    product = complaint_data.get('product', 'N/A')
                    complaint_what_happened = complaint_data.get('complaint_what_happened', 'N/A')
                    date_sent_to_company = complaint_data.get('date_sent_to_company', 'N/A')
                    issue = complaint_data.get('issue', 'N/A')
                    sub_product = complaint_data.get('sub_product', 'N/A')
                    zip_code = complaint_data.get('zip_code', 'N/A')
                    tags = complaint_data.get('tags', 'N/A')
                    has_narrative = complaint_data.get('has_narrative', 'N/A')
                    complaint_id = complaint_data.get('complaint_id', 'N/A')
                    timely = complaint_data.get('timely', 'N/A')
                    consumer_consent_provided = complaint_data.get('consumer_consent_provided', 'N/A')
                    company_response = complaint_data.get('company_response', 'N/A')
                    submitted_via = complaint_data.get('submitted_via', 'N/A')
                    company = complaint_data.get('company', 'N/A')
                    date_received = complaint_data.get('date_received', 'N/A')
                    state = complaint_data.get('state', 'N/A')
                    consumer_disputed = complaint_data.get('consumer_disputed', 'N/A')
                    company_public_response = complaint_data.get('company_public_response', 'N/A')
                    sub_issue = complaint_data.get('sub_issue', 'N/A')
                    # print("Product = ", product)


                    # Write the row to the CSV file
                    writer.writerow([
                        product, complaint_what_happened, date_sent_to_company, issue, sub_product, zip_code, 
                        tags, has_narrative, complaint_id, timely, consumer_consent_provided, company_response, 
                        submitted_via, company, date_received, state, consumer_disputed, company_public_response, 
                        sub_issue
                    ])
                    print(f"State {state} has been fetched!")
            else:
                logging.error(f"Failed to fetch data for {state}: {response.status_code}")

        print("Data saved to consumer_complaints.csv")
        kwargs['ti'].xcom_push(key='file_path', value='/home/muneeb/airflow/dags/consumer_complaints.csv')
        print(f"File path complaints.csv has been pushed to XCOM")

