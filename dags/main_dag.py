import psycopg2
from psycopg2 import sql
import sys
import os
import re
import pandas as pd
import logging

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

#initialise path
sys.path.append(os.getcwd())



#------Extract Functions--------#

def extract_csv_filenames(dir ='data/external-funds/external-funds' ):


    '''
    Function to extract all csv file names from the external-funds directory

    Input: 
    - String dir: relative path to external-fund csvs
    Output: 
    - Dataframe file_df: containing list of external funds files where extraction is to be performed (both relative and full path)

    
    '''
    path_to_dir = os.path.join(os.getcwd(), dir)
    filenames = os.listdir(path_to_dir)
    files = [filename for filename in filenames if filename.endswith( ".csv" ) ]
    full_files = [ os.path.join(path_to_dir,filename) for filename in filenames if filename.endswith( '.csv' ) ]
    file_df = pd.DataFrame(list(zip(files,full_files)), columns=['filename', 'full_path_filename'])



    return file_df


def extract_fundname(filename):
   
   '''
   Function to extract fund name from file conventions via regexp

   Input: 
   - String filename: File directory name
   Output: 
   - String matches: Matching fund name
   
   
   '''
   reg = r'^[^.]*' #get all characters before '.'
   reg_sub = r'[\W_]' #replace all special characters with spaces
   matches = re.sub(reg_sub, ' ',re.findall(reg, filename)[0]).split(' ')[-1]
   return matches


def extract_date(filename):
   '''
   Function to extract datetime values from file conventions via regexp

   Input: 
   - String filename: File directory name
   Output: 
   - String matches: Matching datetime values
   
   
   '''
   reg = r'\.(.*)'  #get all characters after '.'
   reg_sub = r'\D*$' #get all numerical numbers
   matches = re.sub(reg_sub,'',re.findall(reg, filename)[0])

   matches = matches.replace("_", "-") #account for "_" string format

   return matches
      

def extract(**kwargs):
   '''
   Main extract function to consolidate all fund data with two additional columns added (date and fund name)


   Input: 
   - Timestamp in Epoch of load time. Serves as unique identifier for output files.
   Output:
   - CSV File control_table: list of files to be ingested.
   
   
   '''
   
   load_time = kwargs['load_time']

   #extract fund names and dates
   file_df = extract_csv_filenames()
   file_df['extracted_filename_fund_name']=file_df['filename'].apply(lambda x: extract_fundname(x))
   file_df['extracted_filename_date']= file_df['filename'].apply(lambda x: extract_date(x))

   #convert datetime to uniform format and additional columns
   file_df['date'] = pd.to_datetime(file_df['extracted_filename_date'], format='mixed')
   file_df['load_datetime_utc'] = pd.Timestamp.utcnow()
   file_df['hash_uid'] = pd.util.hash_pandas_object(file_df)

   #[TO BE STATE IN PRODUCTION WHEN SCHEDULE FOLLOWS MONTHLY CADENCE]
   #filter by current latest month in date column of file_df

   
   file_df.to_csv(os.path.join(os.getcwd(), 'data/output/control_table_{}.csv').format(load_time),index=False)

   return file_df


#---Transform Functions---#


def transform(**kwargs):
   '''
   Main extract function to consolidate all fund data with two additional columns added (date and fund name)
   To be state in production - 

   Input: 
   - Timestamp in Epoch of load time. Serves as unique identifier for output files.
   Output:
   - CSV File control_table: list of files to be ingested.
   
   
   '''
   load_time = kwargs['load_time']
   load_config_df = pd.read_csv(os.path.join(os.getcwd(), 'data/output/control_table_{}.csv'.format(load_time)))

   payload = []

   for i, load_row in load_config_df.iterrows():
      print("File No. {}. Initiating load for {}, extract date {} /n Filename: {}".format(i,load_row['extracted_filename_fund_name'], \
                                                                            load_row['extracted_filename_date'], \
                                                                            load_row['full_path_filename']))
      try:
         current_df = pd.read_csv(load_row['full_path_filename'])
         current_df['fund_name'] = load_row['extracted_filename_fund_name']
         current_df['date'] = load_row['date'] 
         current_df['hash_uid'] = str(load_row['hash_uid'])

         payload.append(current_df)
        
      except Exception as e:
         print("Load for {} failed./n Exception:{}".format(load_row['full_path_filename'], {e}))
         logging.error(e)
        
   merged_df = pd.concat(payload)
   output_file_path = os.path.join(os.getcwd(), 'data/output/landing_file_{}.csv'.format(load_time))

   merged_df.columns = [ col.replace(' ', '_').replace('/', '').lower() for col in merged_df.columns]
   merged_df.to_csv(output_file_path, index=False)
   


   return merged_df, output_file_path

#---Load Functions---#
def load(**kwargs):
   
   '''
   Load transformed table into postgresql database

   Input: 
   - Timestamp in Epoch of load time. Identifies the current load's landing file.
   Output:
   - Ingested table on Postgres + status return
   
   
   '''
   
   #read landing file exported from transformed dataset
   load_time = kwargs['load_time']
   file_path = os.path.join(os.getcwd(), 'data/output/landing_file_{}.csv'.format(load_time))
   df =  pd.read_csv(file_path)
   
   #specifying col names for copy query
   col_names = ','.join(list(df.columns))
   print(col_names)


   
   #establish postgres connection
   try: 
      conn = psycopg2.connect(
         database="postgres", 
         user='airflow', 
         password='airflow', 
         host = 'host.docker.internal',
         port= '5432'
      )
      conn.autocommit=True
      conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
      
      print('Connection successful.')
   except Exception as e:
      print(e)
      logging.error(e)

   curs = conn.cursor()
   
   #execute copy statement
   try:
      curs.copy_expert("COPY raw_external_fund({}) FROM STDIN WITH DELIMITER ',' CSV HEADER".format(col_names), open(file_path, "r"))
   except (Exception, psycopg2.DatabaseError) as error:
    print("Error: %s" % error)
    logging.error(error)
    conn.rollback()
    curs.close()
    return 1
   print('Copy successful into raw_external_fund.')


   curs.close()
   conn.close()



#----AirFlow DAG Definitions----#
with DAG(
    "gic_main_dag",

    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule_interval='0 0 1 * *', 
    start_date=datetime(2021, 1, 1),  #assumes file delivery happen at end of each month and ingestion happens T+1
    catchup=False,
    tags=["assessment"],
    params={"load_time":pd.Timestamp.utcnow().value}
) as dag:


    #task to initialise reference table in Postgresql
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

    #task to perform extract step
    t3 = PythonOperator(
      task_id = "extract",
      python_callable=extract,
      op_kwargs={"load_time": "{{params.load_time}}"}  
    )

    #task to perform transform step
    t4 = PythonOperator(
      task_id = "transform",
      python_callable=transform,
      op_kwargs={"load_time": "{{params.load_time}}"}  
    )

    #task to perform load step
    t5 = PythonOperator(
      task_id = "load",
      python_callable=load,
      op_kwargs={"load_time": "{{params.load_time}}"}
    )


    t1>>t2>>t3>>t4>>t5