from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

class LinkedinJobScraper:
    BASE_URL = "https://www.linkedin.com/jobs/search/?keywords=analyst&location=Australia"
        
    def __init__(self, output_path="/opt/airflow/include/data_files/raw", page_size=50):
        # Use absolute path inside the container. Ensure the directory exists so
        # pandas.to_csv won't fail with a "non-existent directory" OSError.
        self.output_path = output_path
        self.PAGE_SIZE = page_size

    def scrape_linkedin_jobs(self):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        if self.output_path.startswith("/opt/airflow"):
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)
            
        driver.get(self.BASE_URL)
        driver.implicitly_wait(10)
        wait = WebDriverWait(driver, 10)

        dismiss_buttons = driver.find_elements(By.CSS_SELECTOR, 'button[aria-label="Dismiss"]')

        # Hide the sign in window
        print(len(dismiss_buttons))
        dismiss_buttons[len(dismiss_buttons)-2].click()

        # Filter for "Entry level" jobs
        experience_filter = driver.find_element(
            By.XPATH, '//button[contains(@aria-label, "Experience level filter")]'
        )
        experience_filter.click()

        # Wait for the options container
        options_container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".filter-values-container__filter-values"))
        )

        # Wait for the label to appear and be clickable
        internship_label = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'label[for="f_E-0"]'))
        )
        internship_label.click()

        entry_label = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'label[for="f_E-1"]'))
        )
        entry_label.click()

        # Click the submit button to apply the filter
        submit_button = driver.find_element(By.CLASS_NAME, "filter__submit-button")
        driver.execute_script("arguments[0].click();", submit_button)

        # Filter for past month
        experience_filter = driver.find_element(
            By.XPATH, '//button[contains(@aria-label, "Date posted filter options")]'
        )
        experience_filter.click()

        # Wait for the options container
        options_container = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".filter-values-container__filter-values"))
        )

        # Wait for the label to appear and be clickable
        past_month = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'label[for="f_TPR-1"]'))
        )
        past_month.click()
        submit_button = driver.find_element(By.CLASS_NAME, "filter__submit-button")
        driver.execute_script("arguments[0].click();", submit_button)

        # Scroll down to load more jobs
        while (not driver.find_element(By.CSS_SELECTOR, "button[aria-label='See more jobs']").is_displayed()):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        for _ in range(self.PAGE_SIZE):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            try:
                see_more_button = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='See more jobs']"))
                )
                see_more_button.click()
            except:
                print("No more 'See more jobs' button found or button not clickable.")
                break

        # Extract job listings including job title, company name, location, and link to job description
        job_listings = driver.find_elements(By.CLASS_NAME, 'base-card')
        print(f"Found {len(job_listings)} job listings.")

        job_data = []

        for job in job_listings:

            try:
                job_des_link = job.find_element(By.CLASS_NAME, 'base-card__full-link').get_attribute('href')
                job_title = job.find_element(By.CLASS_NAME, 'base-card__full-link').find_element(By.CLASS_NAME, 'sr-only').text

                job_detail = job.find_element(By.CLASS_NAME, 'base-search-card__info')
                company_name = job_detail.find_element(By.CLASS_NAME, 'hidden-nested-link').text
                job_location = job_detail.find_element(By.CLASS_NAME, 'job-search-card__location').text
                job_data.append({
                    'Job Title': job_title,
                    'Company Name': company_name,
                    'Location': job_location,
                    'Job Link': job_des_link,
                    'Date Scraped': pd.Timestamp.now().strftime("%Y-%m-%d")
                })  
            except:
                continue

        df = pd.DataFrame(job_data)
        df.to_csv(f'{self.output_path}/linkedin_jobs.csv')

        with open(f"{self.output_path}/linkedin_jobs.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)

        print("Scrape Linkedin Jobs: Done")
        driver.quit()

        
    def extract_job_descriptions(self):
        # Read job_url
        job_listings = pd.read_csv(f'{self.output_path}/linkedin_jobs.csv')
        print(f"Read {len(job_listings)} rows from job listings")
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


                if job_des_text:
                    job_descriptions.append([job_url, job_des_text.strip()])

            else:
                job_descriptions.append([job_url, ""])  # append empty description, same list


        pd.DataFrame(job_descriptions, columns=["Job Link","Raw Job Description"]).to_csv(f'{self.output_path}/linkedin_job_descriptions.csv')
        print(f"Extracted {len(job_descriptions)} job descriptions")
