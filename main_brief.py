import os
import pandas as pd
from Ongoing_Crawl99designBrief import download_brief


def main():
    # Set base paths
    output_dir_list = "./Image_99Design/ContestList/OngoingFrom241004"  # Path for contest list CSV
    output_dir_Image = "./Image_99Design/ContestImage/OngoingFrom241004"  # Path for images and summary CSV

    # Ensure output directories exist
    os.makedirs(output_dir_list, exist_ok=True)
    os.makedirs(output_dir_Image, exist_ok=True)

    # Path for aggregated CSV file
    all_contests_csv = os.path.join(output_dir_Image, "OngoingContest_brief1004to1020.csv")

    # Step 2: Load contest data from CSV
    contests_df_path = os.path.join(output_dir_list, "Contest_URL_All_1004to1020.csv")
    contests_df = pd.read_csv(contests_df_path, dtype=str)

    # Load existing summary data if the file exists
    if os.path.exists(all_contests_csv):
        all_contests_df = pd.read_csv(all_contests_csv)
    else:
        all_contests_df = pd.DataFrame()

    # Step 4: Iterate through each contest
    for index, row in contests_df.iterrows():
        contest_url = row['ContestURL']
        contest_id = row['ContestID']
        contest_name = row['ContestName']

        # Create a directory for each contest
        contest_output_dir = os.path.join(output_dir_Image, contest_id)
        os.makedirs(contest_output_dir, exist_ok=True)

        # Step 5: Download images for the current contest
        print(f"Downloading Brief for contest {contest_name} (ID: {contest_id})...")
        csv_filename = f"{contest_id}_contest.csv"
        download_brief(contest_url, contest_output_dir, csv_filename)

        # Step 6: Append current contest's data to the aggregated DataFrame
        contest_csv_path = os.path.join(contest_output_dir, f"Submission_Contestant_{csv_filename}.csv")
        if os.path.exists(contest_csv_path):
            contest_df = pd.read_csv(contest_csv_path)
            all_contests_df = pd.concat([all_contests_df, contest_df], ignore_index=True)

            # Save interim progress to prevent data loss
            all_contests_df.to_csv(all_contests_csv, index=False)

    # Step 7: Final save of the aggregated data and remove duplicates
    all_contests_df.drop_duplicates(subset='DesignID', keep='first', inplace=True)
    all_contests_df.to_csv(all_contests_csv, index=False)

    print(f"Aggregated CSV file saved as {all_contests_csv}")


if __name__ == "__main__":
    main()
