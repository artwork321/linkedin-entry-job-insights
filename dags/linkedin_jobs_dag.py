from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
import sys

sys.path.append("/opt/airflow/include")
import scrape_linkedin, data_processing, upload_data

def scrape_jobs():
    scraper = scrape_linkedin.LinkedinJobScraper(page_size=1)

    scraper.scrape_linkedin_jobs()
    scraper.extract_job_descriptions()

def clean_jobs():
    cleaner = data_processing.LinkedinJobCleaner()
    cleaner.clean_job_descriptions()
    cleaner.clean_job_details()

def upload_snowflake():
    upload_data.upload_to_snowflake()

with DAG(dag_id="linkedin_jobs", start_date=datetime(2025, 10, 11), schedule='@monthly', catchup=False) as dag:
    
    # Step 1: Extract job lists
    scrape_web = PythonOperator(
        task_id="scrape_web",
        python_callable=scrape_jobs,
    )

    # Step 2: Extract and clean job descriptions
    clean_job_description = PythonOperator(
        task_id="clean_job_details",
        python_callable=clean_jobs,
    )

    # Step 3: Upload to Snowflake
    upload_snowflake = PythonOperator(
        task_id="upload_snowflake",
        python_callable=upload_snowflake,
    )

    scrape_web >> clean_job_description >> upload_snowflake


