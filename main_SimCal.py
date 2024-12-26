from Ongoing_IMageSimDL import load_vgg16_model, extract_entry_number, calculate_similarities, save_aggregated_stats
import os
import pandas as pd
from PIL import UnidentifiedImageError, ImageFile
from PIL import Image

# Allow loading truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True
Image.MAX_IMAGE_PIXELS = None

def main():
    contest_dir = './Image_99Design/ContestImage/OngoingFrom241004'  
    agg_output_file = os.path.join(contest_dir, 'SimiContestFinal.csv')  # Output file for aggregated similarity statistics
    model = load_vgg16_model()  # Loading the VGG16 model

    # Load the OngoingContest_1004to1020.csv and c'x'd sort by ContestIDZ in descending order
    contest_csv = pd.read_csv('./Image_99Design/ContestList/OngoingFrom241004/ContestFinal.csv')
    contest_csv = contest_csv.sort_values(by='ContestID', ascending=False)

    # Get the values from the ContestID column to be used as valid folder names for processing
    valid_contest_ids = set(contest_csv['ContestID'].astype(str))  # Convert to string to match folder names

    # Read and sort contest folder names, only processing valid folders
    contest_list = sorted(
        [contest for contest in os.listdir(contest_dir) if os.path.isdir(os.path.join(contest_dir, contest)) and contest in valid_contest_ids],
        key=int, reverse=True)

    print(f"Found {len(contest_list)} valid Contests")

    # Iterate through each valid contest folder
    for contest in contest_list:
        contest_path = os.path.join(contest_dir, contest)
        contest_id = contest  # Use the folder name as contest_id

        try:
            # Calculate similarities using VGG16, SIFT, and Color Histogram
            entry_files, sim_matrix_vgg, sim_matrix_sift, sim_matrix_color = calculate_similarities(contest_path, model)

            # If there are valid similarity matrices, save the results
            if sim_matrix_vgg is not None and sim_matrix_sift is not None and sim_matrix_color is not None:
                # Save aggregated statistics for VGG16, SIFT, and Color Histogram
                ref_count = len(sim_matrix_vgg[0]) - len(entry_files)
                save_aggregated_stats(contest_id, entry_files, sim_matrix_vgg, sim_matrix_sift, sim_matrix_color, ref_count, agg_output_file)

        except (UnidentifiedImageError, OSError, IOError) as e:
            # Handle UnidentifiedImageError, OSError, and IOError by skipping the contest
            print(f"Skipping contest {contest_id} due to error: {str(e)}")
            # You can also choose to log the failed contest in a summary file or print an error message here.

if __name__ == '__main__':
    main()
