import snowflake.connector
import pandas as pd 
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

def upload_to_snowflake(input_path="/opt/airflow/include/data_files/curated"):
    
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

    # Create sequences
    cur.execute("""
    CREATE SEQUENCE IF NOT EXISTS linkedin_jobs.job_details.job_details_seq START = 1 INCREMENT = 1 ORDER;
    """)
    
    cur.execute("""
    CREATE SEQUENCE IF NOT EXISTS linkedin_jobs.job_descriptions.job_descriptions_seq START = 1 INCREMENT = 1 ORDER;
    """)

    # Create tables with default sequence values instead of AUTOINCREMENT
    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_jobs.job_details.t_job_details (
        id INT DEFAULT linkedin_jobs.job_details.job_details_seq.NEXTVAL PRIMARY KEY,
        job_title VARCHAR(255),
        company_name VARCHAR(255),
        location VARCHAR(255),
        job_link VARCHAR(500),
        date_scraped DATE,
        city VARCHAR(255),
        state VARCHAR(255)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS linkedin_jobs.job_descriptions.t_job_descriptions (
        id INT DEFAULT linkedin_jobs.job_descriptions.job_descriptions_seq.NEXTVAL PRIMARY KEY,
        job_link VARCHAR(500),
        job_description STRING,
        skills STRING
    )
    """)

    # Load data from CSV files
    job_descriptions_df = pd.read_csv(f'{input_path}/linkedin_job_descriptions_with_skills.csv', index_col=0).reset_index(drop=True)
    job_details_df = pd.read_csv(f'{input_path}/linkedin_jobs.csv', index_col=0).reset_index(drop=True)
    job_descriptions_df.columns = job_descriptions_df.columns.str.strip().str.upper()
    job_details_df.columns = job_details_df.columns.str.strip().str.upper()

    # Upload data to Snowflake
    # Create a stage
    cur.execute("""CREATE OR REPLACE STAGE linkedin_jobs.job_details.linkedin_d_stage;""")
    cur.execute("""CREATE OR REPLACE STAGE linkedin_jobs.job_descriptions.linkedin_jd_stage;""")

    # Upload csv files to stage
    cur.execute(f"""PUT file://{input_path}/linkedin_jobs.csv @linkedin_jobs.job_details.linkedin_d_stage""")
    cur.execute(f"""PUT file://{input_path}/linkedin_job_descriptions_with_skills.csv @linkedin_jobs.job_descriptions.linkedin_jd_stage""")

    # Create temp tables WITHOUT ID column
    cur.execute("""
    CREATE OR REPLACE TEMP TABLE linkedin_jobs.job_details.tmp_jobs (
        job_title VARCHAR(255),
        company_name VARCHAR(255),
        location VARCHAR(255),
        job_link VARCHAR(500),
        date_scraped DATE,
        city VARCHAR(255),
        state VARCHAR(255)        
    );
    """)
    
    cur.execute("""
    CREATE OR REPLACE TEMP TABLE linkedin_jobs.job_descriptions.tmp_jobs (
        job_link VARCHAR(500),
        job_description STRING,
        skills STRING
    );
    """)

    upload_stage = """
    COPY INTO linkedin_jobs.job_details.tmp_jobs
    FROM (
        SELECT $2, $3, $4, $5, $6, $7, $8
        FROM @linkedin_jobs.job_details.linkedin_d_stage
    )
    FILE_FORMAT = (
        TYPE='CSV',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        TRIM_SPACE=TRUE
    );
    """

    upload_stage2 = """
    COPY INTO linkedin_jobs.job_descriptions.tmp_jobs
    FROM (
        SELECT $2, $3, $4
        FROM @linkedin_jobs.job_descriptions.linkedin_jd_stage
    )
    FILE_FORMAT = (
        TYPE='CSV',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        TRIM_SPACE=TRUE
    );
    """
    cur.execute(upload_stage)
    cur.execute(upload_stage2)

    print("Successfully Upload data to stage and temp table")

    # Upload only new rows - sequence only increments on actual INSERT
    merge_query = """
    MERGE INTO linkedin_jobs.job_details.t_job_details j
    USING linkedin_jobs.job_details.tmp_jobs t
    ON j.job_link = t.job_link
    WHEN NOT MATCHED THEN
        INSERT (job_title, company_name, location, job_link, date_scraped, city, state)
        VALUES (t.job_title, t.company_name, t.location, t.job_link, t.date_scraped, t.city, t.state);
    """

    merge_query2 = """
    MERGE INTO linkedin_jobs.job_descriptions.t_job_descriptions j
    USING linkedin_jobs.job_descriptions.tmp_jobs t
    ON j.job_link = t.job_link
    WHEN NOT MATCHED THEN
        INSERT (job_link, job_description, skills)
        VALUES (t.job_link, t.job_description, t.skills);
    """
    cur.execute(merge_query)
    cur.execute(merge_query2)

    print("Successfully upload new data to table")
    
    # Close connection
    cur.close()
    conn.close()