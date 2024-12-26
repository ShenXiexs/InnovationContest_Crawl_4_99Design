import os
import pandas as pd
from Ongoing_Crawl99designPage import scrape_contests
from Ongoing_Crawl99design import download_images


def main():
    # Set base path
    output_dir_list = "your crawl list"  # Path to save contest list CSV
    output_dir_Image = "your store path"

    # Ensure output directories exist
    os.makedirs(output_dir_Image, exist_ok=True)  # Create directory to save images

    # Define path for the aggregated CSV file
    all_contests_csv = os.path.join(output_dir_Image, "OngoingContest_1212_2.csv")  # Path for aggregated CSV file

    # Step 2: Load the contest data CSV file that was scraped

    contests_df_path = os.path.join(output_dir_list, "Contest_URL_20241212_234835.csv")  # Path to scraped results CSV
    contests_df = pd.read_csv(contests_df_path)
    contests_df = contests_df.astype(str)  # Convert all columns to string type

    # If the aggregated CSV file already exists, load existing data; otherwise, create an empty DataFrame
    if os.path.exists(all_contests_csv):
        all_contests_df = pd.read_csv(all_contests_csv)
    else:
        all_contests_df = pd.DataFrame()

    # Step 4: Iterate through each contest and download images
    for index, row in contests_df.iterrows():
        contest_url = row['ContestURL']
        contest_id = row['ContestID']
        contest_name = row['ContestName']

        # Create a specific directory for each contest to save images
        contest_output_dir = os.path.join(output_dir_Image, contest_id)  # Path to save each contest's images
        if not os.path.exists(contest_output_dir):
            os.makedirs(contest_output_dir)

        # Step 5: Download images for the contest
        print(f"Downloading images for contest {contest_name} (ID: {contest_id})...")
        csv_filename = f"{contest_id}_contest.csv"  # Filename to save contest images CSV
        download_images(contest_url, contest_output_dir, csv_filename)

        # Step 6: Load the image data CSV for the current contest and merge it into the aggregated DataFrame
        contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")  # Full path to contest image CSV
        if os.path.exists(contest_csv_path):
            contest_df = pd.read_csv(contest_csv_path)
            all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)

            # Temporarily save current progress to prevent data loss in case of failure
            all_contests_df.to_csv(all_contests_csv, index=False)

    # Step 7: Remove duplicates based on DesignID, sort by ContestID and Entry, and save final CSV
    all_contests_df.drop_duplicates(subset='DesignID', keep='first', inplace=True)  # Remove duplicates based on DesignID
    # Sort by ContestID (descending) and Entry (descending within each ContestID)
    all_contests_df.sort_values(by=['ContestID', 'Entry'], ascending=[False, False], inplace=True)
    all_contests_df.to_csv(all_contests_csv, index=False)

    print(f"Aggregated CSV file saved as {all_contests_csv}")


if __name__ == "__main__":
    main()

