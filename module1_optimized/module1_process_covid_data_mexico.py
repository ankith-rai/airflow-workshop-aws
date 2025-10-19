""" Copyright Amazon.com, Inc. or its affiliates. 
 All Rights Reserved.SPDX-License-Identifier: MIT-0"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import pendulum

COUNTRY = 'Mexico'

with DAG(
    dag_id="top_level_code_mexico",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["Module1"],
) as dag:

    @task()
    def process_covid_data():
        
        ### Local imports ###
        import io
        import boto3
        import pandas as pd
        import numpy as np

        ### Local Variable access ###
        S3_BUCKET = Variable.get("S3_BUCKET_NAME", "")
        INPUT_KEY = Variable.get("INPUT_KEY","")
        OUTPUT_KEY = Variable.get("OUTPUT_KEY","")

        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET, Key=INPUT_KEY)

        status = response['ResponseMetadata']['HTTPStatusCode']

        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
            df.drop(['Last_Update', 'Lat', 'Long_', 'Recovered','Active','Combined_Key','Incident_Rate','Case_Fatality_Ratio'], axis=1, inplace = True)
            df.rename(columns={'Province_State': 'State', 'Country_Region': 'Country'}, inplace=True)
            df = df.fillna('NA')
            df2 = df[df['Country'] == COUNTRY]
            df3 = df2.groupby('State')[['Confirmed', 'Deaths']].sum().reset_index()
            df3['10KorMore'] = np.where((df3["Deaths"] > 10000), 1, 0)
            df3.sort_values(by='State')

            with io.StringIO() as csv_buffer:
                df3.to_csv(csv_buffer, index=False)

                response = s3.put_object(
                    Bucket=S3_BUCKET, Key=OUTPUT_KEY + '_' + COUNTRY + '.csv', Body=csv_buffer.getvalue()
                )

                status = response['ResponseMetadata']['HTTPStatusCode']
                if status == 200:
                    print(f"Successful S3 put_object response. Status - {status}")
                else:
                    print(f"Unsuccessful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    process_covid_data()
        