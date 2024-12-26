import os
from Ongoing_Crawl99designPage import scrape_contests

# Define the main function
def main():
    # Set basic parameters
    url = "https://99designs.hk/logo-design/contests?sort=start-date%3Adesc&status=open&entry-level=0&mid-level=0&top-level=0&dir=desc&order=start-date"
    output_dir = "./Image_99Design/ContestList/OngoingFrom241004"

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Call the scraping function
    scrape_contests(url, output_dir)


# Run the main function
if __name__ == "__main__":
    main()
