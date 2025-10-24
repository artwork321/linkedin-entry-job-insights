from include import upload_data, data_processing, scrape_linkedin
import spacy



# Run the scraping script to get job listings
scraper = scrape_linkedin.LinkedinJobScraper(page_size=1, output_path="include/data_files/raw")
scraper.scrape_linkedin_jobs()
scraper.extract_job_descriptions()

# Clean job descriptions
cleaner = data_processing.LinkedinJobCleaner(input_path="include/data_files/raw", output_path="include/data_files/curated",
                                             model_path="include/data_files/model/model-last")
cleaner.clean_job_descriptions()
cleaner.clean_job_details()

# Upload data to snowflake
# upload_data.upload_to_snowflake(input_path="include/data_files/curated")