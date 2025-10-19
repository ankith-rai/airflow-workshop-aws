"""Copyright Amazon.com, Inc. or its affiliates.
All Rights Reserved.SPDX-License-Identifier: MIT-0"""

import pendulum
from airflow import DAG

from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    schedule='24 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['Module2'],
) as dag:

    print_dag_list = BashOperator(
        task_id="list_dags",
        bash_command="ls /usr/local/airflow/dags; sleep $((RANDOM % 61))",
    )

    print_dag_list
