import snowflake.connector
import pandas as pd 
import os
from dotenv import load_dotenv

def upload_to_snowflake():
    # Connect to Snowflake
    load_dotenv(override=True) 

    conn = snowflake.connector.connect(
        user=os.getenv("USER").strip(),
        password=os.getenv("PASSWORD").strip(),
        account=os.getenv("ACCOUNT").strip(),
        warehouse=os.getenv("WAREHOUSE").strip(),
        database=os.getenv("DATABASE").strip(),
        schema=os.getenv("SCHEMA").strip(),
        role=os.getenv("ROLE").strip()
    )

    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""CREATE TABLE IF NOT EXISTS linkedin_jobs.job_details.t_job_details (
                id INT AUTOINCREMENT PRIMARY KEY,
                Job Title VARCHAR(255),
                Company Name VARCHAR(255),
                Location VARCHAR(255),
                Job Link VARCHAR
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS linkedin_jobs.job_descriptions.t_job_descriptions (
                id INT AUTOINCREMENT PRIMARY KEY,
                Job Descriotion STRING
    )""")

    # Load data from CSV files
    job_details_df = pd.read_csv('data_files/curated/job_details.csv')
    job_descriptions_df = pd.read_csv('data_files/curated/job_details.csv')
