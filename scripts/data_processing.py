import pandas as pd
import re
import spacy

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

    # Rename columns for consistency
    df.rename(columns={
        'Job Title': 'job_title',
        'Company Name': 'company_name',
        'Location': 'location',
        'Job Link': 'job_link',
        'City': 'city',
        'State': 'state'
    }, inplace=True)

    df.to_csv("data_files/curated/linkedin_jobs.csv", index=False)
    print("Data cleaned and saved to data_files/curated/linkedin_jobs.csv")


def clean_job_descriptions():
    df = pd.read_csv("data_files/raw/linkedin_job_descriptions.csv")
    skills_extracted = []

    for index, row in df.iterrows():
        job_des = row["Raw Job Description"]

        # Lowercase the text
        job_des = job_des.lower()

        # Load model
        model = spacy.load("model/model-last")

        doc = model(job_des)
        skills_extracted.append(set([ent.text for ent in doc.ents]))

        for ent in doc.ents:
            print(ent.text, ent.label_)

    df.rename(columns={
        'Raw Job Description': 'job_description'
    }, inplace=True)
    df["skills"] = skills_extracted

    df.to_csv("data_files/curated/linkedin_job_descriptions_with_skills.csv", index=False)

