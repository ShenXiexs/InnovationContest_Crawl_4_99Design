import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from datetime import datetime
from retrying import retry

@retry(wait_random_min=200, wait_random_max=500, stop_max_attempt_number=10000)
def get_total_pages(soup):
    """Get the total number of pages"""
    pagination_summary = soup.find('span', class_='pagination__item pagination__summary')
    if pagination_summary:
        match = re.search(r'of (\d+)', pagination_summary.get_text(strip=True))
        if match:
            total_pages = int(match.group(1))
            return total_pages
    return 1  # Default return of 1 page


def get_contests_from_page(soup):
    """Parse contest information from the current page and extract additional contest attributes"""
    contests = []

    # Find all contest entries
    contest_items = soup.find_all('div', class_='content-listing__item')

    for item in contest_items:
        # Extract contest link and ID
        contest_link = item.find('a', class_='listing-details__title__link') or item.find('a', class_='content-listing__item__link-overlay')
        if contest_link:
            contest_url = f"https://99designs.hk{contest_link['href']}/entries"
            contest_id_match = re.search(r'contests/[^/]+-(\d+)', contest_link['href'])
            contest_id = contest_id_match.group(1) if contest_id_match else 'N/A'

            # Extract contest name
            contest_name = contest_link.get_text(strip=True)

            # Extract reward amount
            reward_tag = item.find('div', class_='ribbon__text')
            reward = reward_tag.get_text(strip=True) if reward_tag else 'N/A'

            # Extract tags such as Blind, Guaranteed, No generative AI allowed, etc.
            tag_section = item.find_all('div', class_='listing-details__section')
            tags = []
            blind = 0  # Initialize Blind flag

            # Find all <span> tags, extract tag text, and determine Blind information
            for section in tag_section:
                tag_spans = section.find_all('span', class_='listing-details__pill')
                for span in tag_spans:
                    tag_text = span.get_text(strip=True)
                    tags.append(tag_text)
                    if tag_text == 'Blind':
                        blind = 1  # Set blind = 1 if Blind tag is present

            # Generate Tags string
            tags_str = ','.join(tags) if tags else 'NA'

            # Extract the number of current submissions (CurrentIdeas)
            current_ideas = 0  # Initialize submission count
            ideas_section = item.find_all('div', class_='listing-details__stat-item')
            for stat in ideas_section:
                stat_label = stat.find('span', class_='listing-details__stat__label')
                if stat_label:
                    ideas_match = re.search(r'(\d+) designs', stat_label.get_text(strip=True))
                    if ideas_match:
                        current_ideas = int(ideas_match.group(1))  # Extract submission count

            # Add data to the list
            contests.append({
                'ContestID': int(contest_id) if contest_id != 'N/A' else None,
                'ContestURL': contest_url,
                'ContestName': contest_name,
                'Reward': reward,
                'Blind': blind,
                'Tags': tags_str,  # Save all tags as a string
                'CurrentIdeas': current_ideas,  # Number of submissions
                'CrawlTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Record crawl time
            })

    return contests


def get_next_page_url(soup):
    """Get the link to the next page"""
    next_page = soup.find('span', class_='pagination__item pagination--next')
    if next_page:
        next_link = next_page.find('a', class_='pagination__button')
        if next_link:
            # Safely access the href attribute using the get method
            href = next_link.get('href')
            if href:
                return f"https://99designs.hk{href}"
    return None



def scrape_contests(base_url, output_dir):
    """Scrape all contest data and save it to a CSV file"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3861.400'
    }

    all_contests = []  # Store all contest information
    current_url = base_url
    total_pages = None
    current_page = 1

    while current_url:
        print(f"Scraping page {current_page} ... URL: {current_url}")
        response = requests.get(current_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        # Get the total number of pages on the first page
        if total_pages is None:
            total_pages = get_total_pages(soup)
            print(f"Total number of pages to scrape: {total_pages}")

        # Retrieve contest information from the current page
        contests = get_contests_from_page(soup)
        all_contests.extend(contests)
        print(f"Number of contests retrieved from page {current_page}: {len(contests)}")

        # Get the link to the next page
        next_page_url = get_next_page_url(soup)
        if next_page_url:
            current_url = next_page_url
            current_page += 1
        else:
            print(f"No more pages to scrape. Finished at page {current_page}.")
            break

    # Create a DataFrame and sort by ContestID in descending order
    df = pd.DataFrame(all_contests)
    df = df.sort_values(by='ContestID', ascending=False)

    # Remove duplicate rows in ContestID and ContestURL, keeping only the first in each duplicate group
    df = df.drop_duplicates(subset=['ContestID', 'ContestURL'], keep='first')

    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save to a CSV file (include date and time as a filename suffix)
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"Contest_URL_{current_time}.csv"
    csv_filepath = os.path.join(output_dir, csv_filename)
    df.to_csv(csv_filepath, index=False, encoding='utf-8')
    print(f"Data saved to {csv_filepath}")
