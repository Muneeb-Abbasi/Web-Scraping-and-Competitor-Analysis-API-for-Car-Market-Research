import pandas as pd


def transform_data(**kwargs):
    # Pull the file path from XCOM
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='file_path', task_ids='fetch_and_save_data')  # Replace with your extract task id
    
    print(f"File path pulled from XCOM: {file_path}")
    
    if file_path:
        # Step 1: Load data from CSV into a DataFrame
        df = pd.read_csv(file_path)

        df.drop(['complaint_what_happened',
                      'date_sent_to_company',
                      'zip_code',
                      'tags',
                      'has_narrative',
                      'consumer_consent_provided',
                      'consumer_disputed',
                      'company_public_response',

        
        ], axis=1)

        # 1. Convert 'date_received' column to datetime format, coercing errors
        df['date_received'] = pd.to_datetime(df['date_received'], errors='coerce')

        # 2. Now, create a new 'Month Year' column by formatting the datetime values
        df['Month Year'] = df['date_received'].dt.strftime("%Y-%m-%d")

        df['complaint_id'] = df['complaint_id'].fillna('No ID')

        grouped_df = df.fillna('Unknown').groupby(['product', 'issue','sub_product','timely','company_response',
                                                    'submitted_via','company',
                                                    'state','sub_issue']).agg({
            'complaint_id': pd.Series.nunique,
            'Month Year': 'first'  
        }).reset_index()

        # Rename the column to indicate it's a count of distinct complaint IDs
        grouped_df = grouped_df.rename(columns={'complaint_id': 'Count of complaint_id'})

        # 5. Save the transformed DataFrame to a CSV file
        grouped_df.to_csv('/home/muneeb/airflow/dags/transformed_data.csv', index=False)
        kwargs['ti'].xcom_push(key='file_path', value='/home/muneeb/airflow/dags/transformed_data.csv')


        print("Data has beeen tranformed!")


