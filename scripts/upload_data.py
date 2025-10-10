import snowflake.connector
import pandas as pd 
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_jobs.job_details.t_job_details (
        id INT AUTOINCREMENT PRIMARY KEY,
        job_title VARCHAR(255),
        company_name VARCHAR(255),
        location VARCHAR(255),
        job_link VARCHAR(500),
        city VARCHAR(255),
        state VARCHAR(255)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_jobs.job_descriptions.t_job_descriptions (
        id INT AUTOINCREMENT PRIMARY KEY,
        job_description STRING,
        skills STRING
    )
    """)


    # Load data from CSV files
    job_descriptions_df = pd.read_csv('data_files/curated/linkedin_job_descriptions_with_skills.csv', index_col=0).reset_index(drop=True)
    job_details_df = pd.read_csv('data_files/curated/linkedin_jobs.csv', index_col=0).reset_index(drop=True)
    job_descriptions_df.columns = job_descriptions_df.columns.str.strip().str.upper()
    job_details_df.columns = job_details_df.columns.str.strip().str.upper()

    # Upload data to Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn,
        job_details_df,
        table_name='T_JOB_DETAILS',
        schema='JOB_DETAILS'
    )

    print(f"{success}: Uploaded {nrows} rows to linkedin_jobs.job_details.t_job_details")

    success, nchunks, nrows, _ = write_pandas(
        conn,
        job_descriptions_df,
        table_name='T_JOB_DESCRIPTIONS',
        schema='JOB_DESCRIPTIONS'
    )

    print(f"{success}: Uploaded {nrows} rows to linkedin_jobs.job_descriptions.t_job_descriptions")
