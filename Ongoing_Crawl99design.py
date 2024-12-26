import pandas as pd
from retrying import retry
import json
import requests
from bs4 import BeautifulSoup
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Retry decorator to handle retries on SSLError
@retry(wait_random_min=200, wait_random_max=500, stop_max_attempt_number=10000)
def get_real_image_url(entry_url, headers):
    """Fetch the real image URL and creation time by visiting the entry page."""
    response = requests.get(entry_url, headers=headers)
    response.raise_for_status()  # Raise an exception for 4xx/5xx errors
    soup = BeautifulSoup(response.text, 'lxml')

    # Find the real image URL in the <link rel="image_src"> tag
    image_link_tag = soup.find('link', rel='image_src')
    image_url = image_link_tag.get('href') if image_link_tag else None

    # Find the creation time in the page's JavaScript data
    time_created_match = re.search(r'"timeCreatedString":"([^"]+)"', response.text)
    create_time = time_created_match.group(1) if time_created_match else 'N/A'

    if not image_url:
        print(f"No image found on page: {entry_url}")
    return image_url, create_time

def get_total_pages(url, headers):
    """Fetch the total number of pages from the pagination summary."""
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for 4xx/5xx errors
    soup = BeautifulSoup(response.text, 'lxml')

    # Find the pagination summary element
    pagination_summary = soup.find('span', class_='pagination__summary')
    if pagination_summary:
        # Extract the total number of pages using regex
        total_pages_text = pagination_summary.get_text()
        match = re.search(r'of (\d+)', total_pages_text)
        if match:
            return int(match.group(1))  # Return the total number of pages
        else:
            print("Could not extract total pages.")
            return 1
    else:
        print("No pagination summary found.")
        return 1  # If no pagination is found, assume it's a single page

def get_contest_id(url):
    """Extract Contest ID from the URL."""
    match = re.search(r'contests/[^/]+-(\d+)/entries', url)
    if match:
        return match.group(1)
    else:
        return 'N/A'

def extract_rating(entry):
    """Extract the rating value from the star rating system."""
    rating_tag = entry.find('input', {'checked': 'checked'})
    if rating_tag and 'value' in rating_tag.attrs:
        return rating_tag['value']
    return 'N/A'  # Return N/A if no rating found

def extract_winner(entry):
    """Check if the entry has the 'winner' status."""
    winner_tag = entry.find('div', {'data-entry-status': 'winner'})
    return 1 if winner_tag else 0

def get_brief_info(brief_url, headers, contest_id, output_dir):
    """Fetch the brief page, extract PriceUSD, PackageLevel, Guarantee, FastTrack, Industry, Blind, OtherNote, Inspiration count, Reference count, and download inspirations/references. Also extract design style attributes."""
    response = requests.get(brief_url, headers=headers)
    response.raise_for_status()  # Raise an exception for 4xx/5xx errors
    soup = BeautifulSoup(response.text, 'lxml')

    # Extract the purchasePrice and packageName from the brief page
    header_price_data = soup.find('div', id='header-price-data')
    if header_price_data and 'data-initial-props' in header_price_data.attrs:
        data_props_str = header_price_data['data-initial-props']
        try:
            data_props = json.loads(data_props_str.replace('&quot;', '"'))
            price_usd = data_props.get('purchasePrice', 'N/A').replace('US$', '')
            package_level = data_props.get('packageName', 'N/A')
        except json.JSONDecodeError:
            print("Error decoding JSON from data-initial-props")
            price_usd, package_level = 'N/A', 'N/A'
    else:
        price_usd, package_level = 'N/A', 'N/A'

    # Extract Guarantee information
    guarantee_tag = soup.find('div', {'data-meta-guarantee-tooltip-content': True})
    guarantee = 1 if guarantee_tag and "The client has guaranteed to award the prize." in guarantee_tag.text else 0

    # Extract FastTrack information
    fasttrack_tag = soup.find('div', text=re.compile(
        r'Following the open round, the client will select a winning design. There is no refinement stage.'))
    fasttrack = 1 if fasttrack_tag else 0

    # Extract Industry information
    industry_match = re.search(r'industry&quot;:\{&quot;value&quot;:&quot;([a-zA-Z]+)&quot;', response.text)
    industry = industry_match.group(1) if industry_match else 'N/A'

    # Extract Blind information
    blind_tag = soup.find('span', class_='meta-item__label', text='Blind')
    blind = 1 if blind_tag else 0

    # Extract OtherNote information
    notes_match = re.search(r'notes&quot;:\{&quot;value&quot;:&quot;(.*?)&quot;', response.text)
    other_note = notes_match.group(1).replace('&quot;', '"') if notes_match else 'N/A'

    # Extract design style attributes
    style_pattern = re.compile(
        r'&quot;(classicModern|matureYouthful|feminineMasculine|playfulSophisticated|economicalLuxurious|geometricOrganic|abstractLiteral)&quot;:(-?\d)')
    style_values = {
        "classicModern": 'N/A',
        "matureYouthful": 'N/A',
        "feminineMasculine": 'N/A',
        "playfulSophisticated": 'N/A',
        "economicalLuxurious": 'N/A',
        "geometricOrganic": 'N/A',
        "abstractLiteral": 'N/A'
    }

    style_matches = style_pattern.findall(response.text)
    for match in style_matches:
        attribute, value = match
        style_values[attribute] = value

    # Extract the publicId for the image download
    public_id_matches = re.findall(r'&quot;publicId&quot;:&quot;([a-zA-Z0-9]+)&quot;', response.text)
    if not public_id_matches:
        print(f"No images found for contest {contest_id}.")
        return price_usd, package_level, guarantee, fasttrack, industry, blind, other_note, 0, 0, style_values

    # Check for reference public IDs
    references_section = re.search(
        r'References&quot;,&quot;elements&quot;:\{&quot;attachments&quot;:\{&quot;value&quot;:\[\{&quot;publicId&quot;:&quot;([a-zA-Z0-9]+)&quot;',
        response.text)
    references_public_ids = references_section.group(1) if references_section else None

    # Create RefImage folder if it doesn't exist
    ref_image_dir = os.path.join(output_dir, 'RefImage')
    os.makedirs(ref_image_dir, exist_ok=True)

    # Track inspiration and reference counts
    inspiration_count, reference_count = 0, 0

    # Load progress from the previous download attempt
    progress_file = os.path.join(output_dir, f'{contest_id}_download_progress.txt')
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            downloaded_images = set(f.read().splitlines())
    else:
        downloaded_images = set()

    # Download images with retry logic
    for idx, public_id in enumerate(public_id_matches, start=1):
        if public_id in downloaded_images:
            print(f"Skipping already downloaded image: {public_id}")
            continue

        # Determine the image type and filename
        if references_public_ids and public_id == references_public_ids:
            img_filename = f"Ref_{idx}_{contest_id}.png"
            reference_count += 1
        else:
            img_filename = f"Inspiration_{idx}_{contest_id}.png"
            inspiration_count += 1

        # Construct the download URL and image path
        download_url = f"https://99designs.hk/contests/{contest_id}/brief/download/{public_id}"
        img_filepath = os.path.join(ref_image_dir, img_filename)

        # Download the image with retry logic
        retries = 15
        while retries > 0:
            try:
                img_response = requests.get(download_url, headers=headers)
                img_response.raise_for_status()
                with open(img_filepath, 'wb') as img_file:
                    img_file.write(img_response.content)
                print(f"Image {img_filename} downloaded successfully: {download_url}")

                # After successful download, append to progress file
                with open(progress_file, 'a') as f:
                    f.write(public_id + '\n')
                break  # Exit retry loop on success
            except requests.exceptions.RequestException as e:
                retries -= 1
                print(f"Failed to download image {img_filename} (retries left: {retries}): {e}")
                if retries == 0:
                    print(f"Aborting download for image {img_filename} after multiple failures.")
                    return price_usd, package_level, guarantee, fasttrack, industry, blind, other_note, inspiration_count, reference_count, style_values

    return price_usd, package_level, guarantee, fasttrack, industry, blind, other_note, inspiration_count, reference_count, style_values

def get_user_profile_info(user_url, headers):
    """Fetch and extract user profile details."""
    response = requests.get(user_url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'lxml')

    # Extract AggregateRating and AggregateReviews
    aggregate_info = soup.find('span', itemprop='aggregateRating')
    if aggregate_info:
        rating_value = aggregate_info.find('span', itemprop='ratingValue').text.strip()
        review_count = aggregate_info.find('span', itemprop='reviewCount').text.strip()
    else:
        rating_value = 'N/A'
        review_count = 'N/A'

    # Extract StartDate
    start_date_tag = soup.find('span', class_='subtle-text', text=re.compile(r'Member since:'))
    start_date = start_date_tag.text.strip().replace('Member since:', '').strip() if start_date_tag else 'N/A'

    # Extract ContestsWon
    contests_won = soup.find('div', class_='stats-panel__item--first', title=re.compile('Total number of contest prize awards'))
    contests_won = contests_won.find('div', class_='stats-panel__item__value').text.strip() if contests_won else 'N/A'

    # Extract RunnerUp
    runner_up = soup.find('div', class_='stats-panel__item', title=re.compile('Total times named as a contest finalist'))
    runner_up = runner_up.find('div', class_='stats-panel__item__value').text.strip() if runner_up else 'N/A'

    # Extract OnetoOne
    oneto_one = soup.find('div', class_='stats-panel__item', title=re.compile('Total number of 1-to-1 Projects completed'))
    oneto_one = oneto_one.find('div', class_='stats-panel__item__value').text.strip() if oneto_one else 'N/A'

    # Extract RepeatClients
    repeat_clients = soup.find('div', class_='stats-panel__item', title=re.compile('Total number of clients who hired this designer'))
    repeat_clients = repeat_clients.find('div', class_='stats-panel__item__value').text.strip() if repeat_clients else 'N/A'

    # Extract UserTag (from a specific section, excluding languages)
    user_tags_section = soup.find('div', class_='profile__tag-section')
    if user_tags_section:
        user_tags = user_tags_section.find_all('span', class_='pill pill--tag')
        user_tag_list = [tag.text.strip() for tag in user_tags]
        user_tag = ', '.join(user_tag_list) if user_tag_list else 'N/A'
    else:
        user_tag = 'N/A'

    # Extract Languages (if the Languages section exists)
    languages_section = soup.find('h3', class_='heading heading--size4', text=re.compile(r'Languages'))
    if languages_section:
        # Look for the next 'pill-group' after the 'Languages' heading
        languages_group = languages_section.find_next('div', class_='pill-group')
        if languages_group:
            language_tags = languages_group.find_all('span', class_='pill pill--tag')
            language_list = [lang.text.strip() for lang in language_tags]
            languages = ', '.join(language_list) if language_list else 'N/A'
        else:
            languages = 'N/A'
    else:
        languages = 'N/A'

    # Extract Certifications (based on tooltip descriptions and certification tags)
    certifications = []

    # Extract all pill--certification items (e.g. 'Print', 'Brand guide')
    certification_tags = soup.find_all('span', class_='pill pill--tag pill--certification')
    for tag in certification_tags:
        certifications.append(tag.text.strip())  # Add 'Print', 'Brand guide', etc.

    # Extract certification levels (based on tooltip descriptions)
    certification_items = soup.find_all('div', class_='pill-group__item', attrs={'data-tooltip': True, 'title': True})
    for item in certification_items:
        tooltip_title = item.get('title', '')
        span_text = item.find('span', class_=re.compile('pill')).text.strip()
        if 'New or developing professionals on 99designs' in tooltip_title:
            certifications.append(span_text)  # Entry Level
        elif 'Professionals on 99designs that are skilled in the essentials of design' in tooltip_title:
            certifications.append(span_text)  # Mid Level
        elif 'Professionals that have built trust on 99designs with their expert skills and creativity' in tooltip_title:
            certifications.append(span_text)  # Top Level

    # Join all certifications with a comma
    certifications = ', '.join(certifications) if certifications else 'N/A'

    return rating_value, review_count, start_date, contests_won, runner_up, oneto_one, repeat_clients, user_tag, certifications, languages

def download_images(url, output_dir, csv_filename):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3861.400'
    }
    contest_id = get_contest_id(url)
    brief_url = url.replace('/entries', '/brief')
    price_usd, package_level, guarantee, fasttrack, industry, blind, other_note, inspiration_count, reference_count, style_values = get_brief_info(
        brief_url, headers, contest_id, output_dir)

    total_pages = get_total_pages(url, headers)
    print(f"Total pages to download: {total_pages}")

    csv_data = []
    successful_entries = set()
    session = requests.Session()

    def fetch_and_download(page):
        page_url = f"{url}?page={page}"
        print(f"Fetching images from: {page_url}")
        
        for attempt in range(15):  # Retry up to 15 times
            try:
                response = session.get(page_url, headers=headers)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed for {page_url}: {e}")
                if attempt == 14:
                    print(f"Failed to fetch {page_url} after 15 attempts. Skipping page.")
                    return []

        soup = BeautifulSoup(response.text, 'lxml')
        entries = soup.find_all('div', class_='entry entry--linked entry--zoom-linked')
        if not entries:
            print(f"No image entries found on page {page_url}. Skipping...")
            return []

        page_data = []
        for entry in entries:
            entry_number = entry.get('id').split('-')[1]
            if entry_number in successful_entries:
                print(f"Entry {entry_number} already processed. Skipping...")
                continue

            user_id = entry.get('data-user-id')
            design_id = entry.get('data-design-id')
            designer_tag = entry.find('a', class_='entry-owner__designer-name-link')
            user_url = f"https://99designs.hk{designer_tag.get('href')}" if designer_tag else 'N/A'
            user_name = designer_tag.text.strip() if designer_tag else 'N/A'

            # Additional retry mechanism for profile fetching
            retry_count = 0
            while retry_count < 15:
                try:
                    rating_value, review_count, start_date, contests_won, runner_up, oneto_one, repeat_clients, user_tag, certifications, languages = get_user_profile_info(
                        user_url, headers)
                    break
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    print(f"Profile fetch attempt {retry_count} failed for user URL {user_url}: {e}")
                    if retry_count == 15:
                        print(f"Failed to fetch profile info for user {user_id} after 15 attempts. Skipping this entry.")
                        continue

            entry_link_tag = entry.find('a', class_='entry__image__inner')
            if entry_link_tag:
                full_entry_url = f"https://99designs.hk{entry_link_tag.get('href')}"
                
                for attempt in range(15):  # Retry download for the image URL
                    try:
                        real_image_url, create_time = get_real_image_url(full_entry_url, headers)
                        break
                    except requests.exceptions.RequestException as e:
                        print(f"Image URL fetch attempt {attempt + 1} failed for {full_entry_url}: {e}")
                        if attempt == 14:
                            print(f"Failed to fetch image URL after 15 attempts for entry {entry_number}. Skipping image download.")
                            continue

                if real_image_url:
                    img_base_name = f'{entry_number}_{user_id}_entry'
                    if any(f.startswith(img_base_name) for f in os.listdir(output_dir)):
                        print(f"Image {img_base_name} already exists. Skipping download.")
                    else:
                        img_name = f'{img_base_name}.png'
                        img_path = os.path.join(output_dir, img_name)
                        os.makedirs(output_dir, exist_ok=True)
                        
                        for attempt in range(30):  # Retry downloading the image
                            try:
                                img_data = session.get(real_image_url, headers=headers, timeout=10).content
                                with open(img_path, 'wb') as img_file:
                                    img_file.write(img_data)
                                print(f"Image {img_name} downloaded successfully.")
                                break
                            except requests.exceptions.RequestException as e:
                                print(f"Image download attempt {attempt + 1} failed for {real_image_url}: {e}")
                                if attempt == 29:
                                    print(f"Failed to download image after 15 attempts. Skipping image download.")
                    
                    successful_entries.add(entry_number)
                    rating = extract_rating(entry)
                    winner_status = extract_winner(entry)
                    page_data.append([
                        contest_id, price_usd, package_level, guarantee, blind, fasttrack, industry, other_note,
                        inspiration_count, reference_count, create_time, design_id, entry_number, rating,
                        winner_status, real_image_url, user_id, user_name, user_url, rating_value, review_count,
                        start_date, contests_won, runner_up, oneto_one, repeat_clients, user_tag, certifications,
                        languages, style_values['classicModern'], style_values['matureYouthful'],
                        style_values['feminineMasculine'], style_values['playfulSophisticated'],
                        style_values['economicalLuxurious'], style_values['geometricOrganic'], style_values['abstractLiteral']
                    ])
        return page_data

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = [executor.submit(fetch_and_download, page) for page in range(1, total_pages + 1)]
        for future in as_completed(futures):
            result = future.result()
            if result:
                csv_data.extend(result)

    if not csv_data:
        print(f"No images found for contest {contest_id}. Skipping CSV creation.")
        return

    # Step to convert data into DataFrame and sort by 'Entry' column in descending order
    df = pd.DataFrame(csv_data, columns=[
        'ContestID', 'PriceUSD', 'PackageLevel', 'Guarantee', 'Blind', 'Fasttrack', 'Industry', 'OtherNotes',
        'Inspiration', 'Reference', 'CreateTime', 'DesignID', 'Entry', 'Rating', 'Winner', 'Image URL', 'UserID',
        'UserName', 'UserURL', 'AggregateRating', 'AggregateReviews', 'StartDate', 'ContestsWon', 'RunnerUp',
        'OnetoOne', 'RepeatClients', 'UserTag', 'Certifications', 'Languages',
        'ClassicModern', 'MatureYouthful', 'FeminineMasculine', 'PlayfulSophisticated',
        'EconomicalLuxurious', 'GeometricOrganic', 'AbstractLiteral'
    ])

    csv_file_path = os.path.join(output_dir, f'Submission_Contestant_{csv_filename}.csv')
    df.drop_duplicates(subset=['Entry'], inplace=True)  # Ensure no duplicate entries
    df.to_csv(csv_file_path, index=False)
    print(f"CSV file saved as {csv_file_path}.")