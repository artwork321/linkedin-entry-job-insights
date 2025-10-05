from bs4 import BeautifulSoup
import requests
import pandas as pd
import re

def extract_job_descriptions():
    # Read job_url
    job_listings = pd.read_csv('data_files/linkedin_jobs.csv')
    job_descriptions = []

    # Extract job description
    for index, row in job_listings.iterrows():
        job_url = row['Job Link']
        
        response = requests.get(job_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            job_description = soup.find('div', class_='show-more-less-html__markup show-more-less-html__markup--clamp-after-5 relative overflow-hidden')
            
            # Replace some tags to maintain the formatting
            if job_description:
                for br in job_description.find_all("br"):
                    br.replace_with("\n")
                for li in job_description.find_all("li"):
                    li.insert_before("\n- ")
            
            job_des_text = job_description.get_text(separator="", strip=False) if job_description else "No description found"
            job_des_text = re.sub(r'^[ \t]+', '', job_des_text,flags=re.MULTILINE)
            job_des_text = re.sub(r'\n\n\n+', '\n\n', job_des_text)

            job_descriptions.append(job_des_text.strip())

    pd.DataFrame(job_descriptions, columns=["Raw Job Description"]).to_csv('data_files/linkedin_job_descriptions.csv')