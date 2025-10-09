import pandas as pd
import re

def clean_data():
    df = pd.read_csv("data_files/raw/linkedin_jobs.csv")

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Normalize job titles and company name
    # Remove |, \, (), - and extra spaces
    df['Job Title'] = df['Job Title'].str.title().str.strip()
    df['Job Title'] = (df['Job Title'].str.split(r'\s*[|/\-]\s*').str[0] # Remove anything after |, /, -
                   .str.replace(r'\([^)]*\)', '', regex=True) # Remove anything in ()
                   .str.replace(r'\d+', '', regex=True)
                   .str.replace(r'\s+', ' ', regex=True)
                   .str.strip()
                   .str.title())


    df['Company Name'] = df['Company Name'].str.title().str.strip()

    # Extract City and State
    df['City'] = df['Location'].apply(lambda x: x.strip().split(', ')[0] if len(x.strip().split(', ')) > 1 else "")
    df['State'] = df['Location'].apply(lambda x: x.strip().split(', ')[1] if len(x.strip().split(', ')) > 1 else "")

    df.to_csv("data_files/curated/linkedin_jobs.csv", index=False)
    print("Data cleaned and saved to data_files/curated/linkedin_jobs.csv")

def clean_job_descriptions():
    df = pd.read_csv("data_files/raw/linkedin_job_descriptions.csv")

