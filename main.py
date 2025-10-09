from scripts import scrape_linkedin, extract_job_des, data_processing, ner_model

# Run the scraping script to get job listings
# scrape_linkedin.scrape_linkedin_jobs()

# # Extract job descriptions from the scraped job listings
# extract_job_des.extract_job_descriptions()

# # Clean and process the scraped data
# data_processing.clean_data()

# Clean job descriptions
ner_model.prepare_ner_data(input_json_path="data_files/ner_data/ner.json")
# data_processing.clean_job_descriptions()

