import sys
import os
import pandas as pd

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

#initialise path
sys.path.append(os.getcwd())

with DAG(
    "initalise_ddl_dag",

    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Initialise DDL",
    schedule_interval=None, 
    start_date=datetime(2021, 1, 1),  #assumes file delivery happen at end of each month and ingestion happens T+1
    catchup=False,
    tags=["assessment"]
) as dag:
    t1 = SQLExecuteQueryOperator(
        task_id="initialise_reference_table_creation",
        conn_id="cursor",
        sql='scripts/master-reference-sql.sql'
        
        )

    #task for create DDL on Postgres to initialise tables required for external fund data and other views
    t2 = SQLExecuteQueryOperator(
       task_id="initialise_external_data_table",
       conn_id="cursor",
       sql='scripts/load-table-ddl.sql'
    )


    t1>>t2
