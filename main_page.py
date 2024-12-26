import os
from Ongoing_Crawl99designPage import scrape_contests

# Define the main function
def main():
    # Set basic parameters
    url = "https://99designs.hk/logo-design/ COPY CONTESTS LIST URL FROM 99design"
    output_dir = "./your store path"

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Call the scraping function
    scrape_contests(url, output_dir)


# Run the main function
if __name__ == "__main__":
    main()
