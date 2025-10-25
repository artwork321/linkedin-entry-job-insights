import pandas as pd
import spacy

class LinkedinJobCleaner:
    
    def __init__(self, output_path="/opt/airflow/include/data_files/curated", 
                input_path="/opt/airflow/include/data_files/raw",
                model_path="/opt/airflow/include/data_files/model/model-last"):
        self.output_path = output_path
        self.input_path = input_path
        self.model_path = model_path


    def clean_job_details(self):
        df = pd.read_csv(f"{self.input_path}/linkedin_jobs.csv")

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Normalize job titles and company name
        # Remove |, \, (), - and extra spaces
        df['Job Title'] = df['Job Title'].str.title().str.strip()
        df['Job Title'] = (
            df['Job Title']
            .str.replace(r'\s*\([^)]*\)', '', regex=True)
            .str.split(r'\s*[|/\-]\s*').str[0] # Remove anything after |, /, -
            .str.replace(r'\d+', '', regex=True)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
            .str.title()
        )

        print(df.iloc[0])

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
            'Date Scraped': 'date_scraped',
            'City': 'city',
            'State': 'state'            
        }, inplace=True)

        df.to_csv(f"{self.output_path}/linkedin_jobs.csv", index=False)
        print("Data cleaned and saved to data_files/curated/linkedin_jobs.csv")


    def clean_job_descriptions(self):
        df = pd.read_csv(f"{self.input_path}/linkedin_job_descriptions.csv")
        df.drop_duplicates(inplace=True)

        skills_extracted = []

        for index, row in df.iterrows():
            job_des = row["Raw Job Description"]

            if pd.notnull(job_des):
                job_des_str = str(job_des).lower()  # safely cast to string

                # Load model
                model = spacy.load(self.model_path)

                doc = model(job_des_str)
                skills_extracted.append(set([ent.text for ent in doc.ents]))

                for ent in doc.ents:
                    print(ent.text, ent.label_)
            else:
                skills_extracted.append([])  

        df.rename(columns={
            'Job Link': 'job_link',
            'Raw Job Description': 'job_description'
        }, inplace=True)
        df["skills"] = skills_extracted

        df["skills"] = df["skills"].apply(lambda x: ', '.join(x) if x else "")

        df.to_csv(f"{self.output_path}/linkedin_job_descriptions_with_skills.csv", index=False)

        print("Data cleaned and saved to data_files/curated/linkedin_job_descriptions_with_skills.csv")

