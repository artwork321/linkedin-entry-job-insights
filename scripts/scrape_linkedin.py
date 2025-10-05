from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

def scrape_linkedin_jobs():
    driver = webdriver.Chrome()

    driver.get("https://www.linkedin.com/jobs/search/?keywords=analyst&location=Australia")
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

    # Scroll down to load more jobs
    while (not driver.find_element(By.CSS_SELECTOR, "button[aria-label='See more jobs']").is_displayed()):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    for _ in range(50):
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

        job_des_link = job.find_element(By.CLASS_NAME, 'base-card__full-link').get_attribute('href')
        job_title = job.find_element(By.CLASS_NAME, 'base-card__full-link').find_element(By.CLASS_NAME, 'sr-only').text

        job_detail = job.find_element(By.CLASS_NAME, 'base-search-card__info')
        company_name = job_detail.find_element(By.CLASS_NAME, 'hidden-nested-link').text
        job_location = job_detail.find_element(By.CLASS_NAME, 'job-search-card__location').text
        job_data.append({
            'Job Title': job_title,
            'Company Name': company_name,
            'Location': job_location,
            'Job Link': job_des_link
        })  

    df = pd.DataFrame(job_data)
    df.to_csv('data_files/linkedin_jobs.csv')

    with open("data_files/linkedin_jobs.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)

    driver.quit()
