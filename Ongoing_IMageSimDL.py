import os
import numpy as np
from tensorflow.keras.applications import VGG16
from tensorflow.keras.preprocessing import image
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
import cv2  # OpenCV for SIFT
from PIL import UnidentifiedImageError, ImageFile
from PIL import Image

# Allow loading truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True
Image.MAX_IMAGE_PIXELS = None

# Module 1: Load VGG16 Model with ImageNet weights
def load_vgg16_model():
    vgg16 = VGG16(weights='imagenet', include_top=False, pooling='max', input_shape=(224, 224, 3))
    print("VGG16 successfully loaded.")
    return vgg16

# Module 2: Load and process an image using VGG16 with transparency handling
def process_image_vgg16(img_path, model):
    try:
        # Open image using PIL to handle transparency
        img = Image.open(img_path).convert('RGBA')
        background = Image.new('RGBA', img.size, (255, 255, 255, 255))
        img = Image.alpha_composite(background, img).convert('RGB')

        # Resize and preprocess the image for VGG16
        img = img.resize((224, 224))
        img_array = image.img_to_array(img)
        img_array = np.expand_dims(img_array, axis=0)
        img_array /= 255.0
        return model.predict(img_array).flatten()
    except (UnidentifiedImageError, OSError) as e:
        print(f"Skipping image {img_path} due to error: {str(e)}")
        return None  # Skip this image by returning None

# Module 3: Process image using SIFT algorithm with transparency handling
def process_image_sift(img_path):
    try:
        img = Image.open(img_path).convert('RGBA')
        background = Image.new('RGBA', img.size, (255, 255, 255, 255))
        img = Image.alpha_composite(background, img).convert('RGB')
        img = np.array(img)
        img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        sift = cv2.SIFT_create()
        keypoints, descriptors = sift.detectAndCompute(gray, None)
        if descriptors is None:
            return None  # Return None if no descriptors found
        return np.mean(descriptors, axis=0)  # Use mean of descriptors for simplicity
    except (UnidentifiedImageError, OSError, cv2.error) as e:
        print(f"Skipping image {img_path} due to error: {str(e)}")
        return None

# Module 4: Process image using Color Histogram with transparency handling
def process_image_color_histogram(img_path):
    try:
        img = Image.open(img_path).convert('RGBA')
        background = Image.new('RGBA', img.size, (255, 255, 255, 255))
        img = Image.alpha_composite(background, img).convert('RGB')
        img = np.array(img)
        img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)

        hist = cv2.calcHist([img], [0, 1, 2], None, [8, 8, 8], [0, 256, 0, 256, 0, 256])
        hist = cv2.normalize(hist, hist).flatten()
        return hist
    except (UnidentifiedImageError, OSError, cv2.error) as e:
        print(f"Skipping image {img_path} due to error: {str(e)}")
        return None

# Module 5: Calculate similarities using VGG16, SIFT, and Color Histogram
def calculate_similarities(contest_path, vgg_model):
    entry_files = []
    ref_files = []

    # Traverse the contest folder and RefImage subfolder for images
    for root, dirs, files in os.walk(contest_path):
        for file in files:
            if file.endswith('.jpg'):
                if 'RefImage' in root:
                    ref_files.append(os.path.join(root, file))
                elif '_entry' in file:
                    entry_files.append(os.path.join(root, file))

    if len(entry_files) < 1:
        print(f"Not enough entry files to process in {contest_path}")
        return entry_files, None, None, None

    # Sort the entry files by the number before the first underscore in the filename
    entry_files.sort(key=lambda x: extract_entry_number(os.path.basename(x)))

    # Process images with VGG16, SIFT, and Color Histogram for entry files
    entry_embeddings_vgg = [process_image_vgg16(img, vgg_model) for img in entry_files]
    entry_embeddings_sift = [process_image_sift(img) for img in entry_files]
    entry_embeddings_color = [process_image_color_histogram(img) for img in entry_files]

    # Find common valid indices across all embeddings for entry files
    valid_indices = [i for i in range(len(entry_embeddings_vgg)) if
                     entry_embeddings_vgg[i] is not None and
                     entry_embeddings_sift[i] is not None and
                     entry_embeddings_color[i] is not None]

    if not valid_indices:
        print("No valid entries after filtering.")
        return [], None, None, None

    # Filter only valid entries for processing
    entry_files = [entry_files[i] for i in valid_indices]
    entry_embeddings_vgg = [entry_embeddings_vgg[i] for i in valid_indices]
    entry_embeddings_sift = [entry_embeddings_sift[i] for i in valid_indices]
    entry_embeddings_color = [entry_embeddings_color[i] for i in valid_indices]

    # Process images with VGG16, SIFT, and Color Histogram for ref files
    ref_embeddings_vgg = [process_image_vgg16(img, vgg_model) for img in ref_files]
    ref_embeddings_sift = [process_image_sift(img) for img in ref_files]
    ref_embeddings_color = [process_image_color_histogram(img) for img in ref_files]

    # Find common valid indices across all embeddings for ref files
    valid_ref_indices = [i for i in range(len(ref_embeddings_vgg)) if
                         ref_embeddings_vgg[i] is not None and
                         ref_embeddings_sift[i] is not None and
                         ref_embeddings_color[i] is not None]

    if not valid_ref_indices:
        print("No valid reference images after filtering.")
        return entry_files, None, None, None

    # Filter only valid ref files for processing
    ref_files = [ref_files[i] for i in valid_ref_indices]
    ref_embeddings_vgg = [ref_embeddings_vgg[i] for i in valid_ref_indices]
    ref_embeddings_sift = [ref_embeddings_sift[i] for i in valid_ref_indices]
    ref_embeddings_color = [ref_embeddings_color[i] for i in valid_ref_indices]

    if any(embed is None for embed in ref_embeddings_vgg) or any(embed is None for embed in ref_embeddings_sift) or any(embed is None for embed in ref_embeddings_color):
        return entry_files, None, None, None

    sim_matrix_vgg = np.zeros((len(entry_embeddings_vgg), len(entry_embeddings_vgg) + len(ref_embeddings_vgg)))
    sim_matrix_sift = np.zeros((len(entry_embeddings_sift), len(entry_embeddings_sift) + len(ref_embeddings_sift)))
    sim_matrix_color = np.zeros((len(entry_embeddings_color), len(entry_embeddings_color) + len(ref_embeddings_color)))

    # Calculate VGG16 similarities
    for i, entry in enumerate(entry_embeddings_vgg):
        sim_matrix_vgg[i, :len(entry_embeddings_vgg)] = cosine_similarity([entry], entry_embeddings_vgg)[0]
        if len(ref_files) > 0:
            sim_matrix_vgg[i, len(entry_embeddings_vgg):] = cosine_similarity([entry], ref_embeddings_vgg)[0]
        else:
            sim_matrix_vgg[i, len(entry_embeddings_vgg):] = np.nan

    # Calculate SIFT similarities
    for i, entry in enumerate(entry_embeddings_sift):
        sim_matrix_sift[i, :len(entry_embeddings_sift)] = cosine_similarity([entry], entry_embeddings_sift)[0]
        if len(ref_files) > 0:
            sim_matrix_sift[i, len(entry_embeddings_sift):] = cosine_similarity([entry], ref_embeddings_sift)[0]
        else:
            sim_matrix_sift[i, len(entry_embeddings_sift):] = np.nan

    # Calculate Color Histogram similarities
    for i, entry in enumerate(entry_embeddings_color):
        sim_matrix_color[i, :len(entry_embeddings_color)] = cosine_similarity([entry], entry_embeddings_color)[0]
        if len(ref_files) > 0:
            sim_matrix_color[i, len(entry_embeddings_color):] = cosine_similarity([entry], ref_embeddings_color)[0]
        else:
            sim_matrix_color[i, len(entry_embeddings_color):] = np.nan

    sim_matrix_vgg = np.around(sim_matrix_vgg, 4)
    sim_matrix_sift = np.around(sim_matrix_sift, 4)
    sim_matrix_color = np.around(sim_matrix_color, 4)

    return entry_files, sim_matrix_vgg, sim_matrix_sift, sim_matrix_color

# Module 6: Save results with VGG16, SIFT, and Color Histogram data
def save_aggregated_stats(contest_id, entry_files, sim_matrix_vgg, sim_matrix_sift, sim_matrix_color, ref_count, agg_output_file):
    aggregated_data = []

    # Load existing aggregated CSV file if it exists
    if os.path.exists(agg_output_file):
        existing_data = pd.read_csv(agg_output_file)
    else:
        existing_data = pd.DataFrame(columns=['ContestID', 'Entry', 'Before_Avg_Sim_VGG', 'After_Avg_Sim_VGG',
                                              'Ref_Max_VGG', 'Ref_Max_3_VGG', 'Ref_Avg_VGG',
                                              'Before_Avg_Sim_SIFT', 'After_Avg_Sim_SIFT',
                                              'Ref_Max_SIFT', 'Ref_Max_3_SIFT', 'Ref_Avg_SIFT',
                                              'Before_Avg_Sim_Color', 'After_Avg_Sim_Color',
                                              'Ref_Max_Color', 'Ref_Max_3_Color', 'Ref_Avg_Color',
                                              'Before_Avg_Sim_VGG_inSolver', 'Before_Avg_Sim_VGG_outSolver',
                                              'After_Avg_Sim_VGG_inSolver', 'After_Avg_Sim_VGG_outSolver',
                                              'Before_Avg_Sim_SIFT_inSolver', 'Before_Avg_Sim_SIFT_outSolver',
                                              'After_Avg_Sim_SIFT_inSolver', 'After_Avg_Sim_SIFT_outSolver',
                                              'Before_Avg_Sim_Color_inSolver', 'Before_Avg_Sim_Color_outSolver',
                                              'After_Avg_Sim_Color_inSolver', 'After_Avg_Sim_Color_outSolver'])

    for i in range(len(entry_files)):
        entry = extract_entry_number(os.path.basename(entry_files[i]))
        solver_id = os.path.basename(entry_files[i]).split('_')[1]  # Extracting solverID from filename

        # Calculate similarities for VGG, SIFT, and Color Histogram
        before_sim_vgg = np.round(np.mean(sim_matrix_vgg[i, :i]), 4) if i > 0 else 0
        after_sim_vgg = np.round(np.mean(sim_matrix_vgg[i, i + 1:len(entry_files)]), 4) if i < len(entry_files) - 1 else 0
        ref_sim_vgg = sim_matrix_vgg[i, len(entry_files):] if ref_count > 0 else [np.nan] * ref_count

        before_sim_sift = np.round(np.mean(sim_matrix_sift[i, :i]), 4) if i > 0 else 0
        after_sim_sift = np.round(np.mean(sim_matrix_sift[i, i + 1:len(entry_files)]), 4) if i < len(entry_files) - 1 else 0
        ref_sim_sift = sim_matrix_sift[i, len(entry_files):] if ref_count > 0 else [np.nan] * ref_count

        before_sim_color = np.round(np.mean(sim_matrix_color[i, :i]), 4) if i > 0 else 0
        after_sim_color = np.round(np.mean(sim_matrix_color[i, i + 1:len(entry_files)]), 4) if i < len(entry_files) - 1 else 0
        ref_sim_color = sim_matrix_color[i, len(entry_files):] if ref_count > 0 else [np.nan] * ref_count

        ref_max_vgg = np.round(np.max(ref_sim_vgg), 4) if ref_count > 0 else np.nan
        ref_max_3_vgg = np.round(np.mean(np.sort(ref_sim_vgg)[-3:]), 4) if ref_count >= 3 else ref_max_vgg
        ref_avg_vgg = np.round(np.mean(ref_sim_vgg), 4) if ref_count > 0 else np.nan

        ref_max_sift = np.round(np.max(ref_sim_sift), 4) if ref_count > 0 else np.nan
        ref_max_3_sift = np.round(np.mean(np.sort(ref_sim_sift)[-3:]), 4) if ref_count >= 3 else ref_max_sift
        ref_avg_sift = np.round(np.mean(ref_sim_sift), 4) if ref_count > 0 else np.nan

        ref_max_color = np.round(np.max(ref_sim_color), 4) if ref_count > 0 else np.nan
        ref_max_3_color = np.round(np.mean(np.sort(ref_sim_color)[-3:]), 4) if ref_count >= 3 else ref_max_color
        ref_avg_color = np.round(np.mean(ref_sim_color), 4) if ref_count > 0 else np.nan

        # Calculate inSolver and outSolver VGG similarities
        in_solver_vgg = [sim_matrix_vgg[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]
        out_solver_vgg_before = [sim_matrix_vgg[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]
        out_solver_vgg_after = [sim_matrix_vgg[i][j] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]

        before_sim_vgg_in_solver = np.round(np.mean(in_solver_vgg), 4) if in_solver_vgg else np.nan
        after_sim_vgg_in_solver = np.round(np.mean([sim_matrix_vgg[j][i] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]), 4) if any(os.path.basename(entry_files[j]).split('_')[1] == solver_id for j in range(i + 1, len(entry_files))) else np.nan
        before_sim_vgg_out_solver = np.round(np.mean(out_solver_vgg_before), 4) if out_solver_vgg_before else np.nan
        after_sim_vgg_out_solver = np.round(np.mean(out_solver_vgg_after), 4) if out_solver_vgg_after else np.nan

        # Calculate inSolver and outSolver SIFT similarities
        in_solver_sift = [sim_matrix_sift[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]
        out_solver_sift_before = [sim_matrix_sift[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]
        out_solver_sift_after = [sim_matrix_sift[i][j] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]

        before_sim_sift_in_solver = np.round(np.mean(in_solver_sift), 4) if in_solver_sift else np.nan
        after_sim_sift_in_solver = np.round(np.mean([sim_matrix_sift[j][i] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]), 4) if any(os.path.basename(entry_files[j]).split('_')[1] == solver_id for j in range(i + 1, len(entry_files))) else np.nan
        before_sim_sift_out_solver = np.round(np.mean(out_solver_sift_before), 4) if out_solver_sift_before else np.nan
        after_sim_sift_out_solver = np.round(np.mean(out_solver_sift_after), 4) if out_solver_sift_after else np.nan

        # Calculate inSolver and outSolver Color Histogram similarities
        in_solver_color = [sim_matrix_color[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]
        out_solver_color_before = [sim_matrix_color[i][j] for j in range(i) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]
        out_solver_color_after = [sim_matrix_color[i][j] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] != solver_id]

        before_sim_color_in_solver = np.round(np.mean(in_solver_color), 4) if in_solver_color else np.nan
        after_sim_color_in_solver = np.round(np.mean([sim_matrix_color[j][i] for j in range(i + 1, len(entry_files)) if os.path.basename(entry_files[j]).split('_')[1] == solver_id]), 4) if any(os.path.basename(entry_files[j]).split('_')[1] == solver_id for j in range(i + 1, len(entry_files))) else np.nan
        before_sim_color_out_solver = np.round(np.mean(out_solver_color_before), 4) if out_solver_color_before else np.nan
        after_sim_color_out_solver = np.round(np.mean(out_solver_color_after), 4) if out_solver_color_after else np.nan

        # Append VGG16, SIFT, and Color Histogram data
        aggregated_data.append([contest_id, entry, before_sim_vgg, after_sim_vgg, ref_max_vgg, ref_max_3_vgg, ref_avg_vgg,
                                before_sim_sift, after_sim_sift, ref_max_sift, ref_max_3_sift, ref_avg_sift,
                                before_sim_color, after_sim_color, ref_max_color, ref_max_3_color, ref_avg_color,
                                before_sim_vgg_in_solver, before_sim_vgg_out_solver,
                                after_sim_vgg_in_solver, after_sim_vgg_out_solver,
                                before_sim_sift_in_solver, before_sim_sift_out_solver,
                                after_sim_sift_in_solver, after_sim_sift_out_solver,
                                before_sim_color_in_solver, before_sim_color_out_solver,
                                after_sim_color_in_solver, after_sim_color_out_solver])

    if aggregated_data:
        agg_df = pd.DataFrame(aggregated_data, columns=['ContestID', 'Entry', 'Before_Avg_Sim_VGG', 'After_Avg_Sim_VGG',
                                                        'Ref_Max_VGG', 'Ref_Max_3_VGG', 'Ref_Avg_VGG',
                                                        'Before_Avg_Sim_SIFT', 'After_Avg_Sim_SIFT',
                                                        'Ref_Max_SIFT', 'Ref_Max_3_SIFT', 'Ref_Avg_SIFT',
                                                        'Before_Avg_Sim_Color', 'After_Avg_Sim_Color',
                                                        'Ref_Max_Color', 'Ref_Max_3_Color', 'Ref_Avg_Color',
                                                        'Before_Avg_Sim_VGG_inSolver', 'Before_Avg_Sim_VGG_outSolver',
                                                        'After_Avg_Sim_VGG_inSolver', 'After_Avg_Sim_VGG_outSolver',
                                                        'Before_Avg_Sim_SIFT_inSolver', 'Before_Avg_Sim_SIFT_outSolver',
                                                        'After_Avg_Sim_SIFT_inSolver', 'After_Avg_Sim_SIFT_outSolver',
                                                        'Before_Avg_Sim_Color_inSolver', 'Before_Avg_Sim_Color_outSolver',
                                                        'After_Avg_Sim_Color_inSolver', 'After_Avg_Sim_Color_outSolver'])

        combined_df = pd.concat([existing_data, agg_df], ignore_index=True)
        combined_df.to_csv(agg_output_file, index=False)
        print(f"Saved aggregated results to {agg_output_file}")

# Helper function to extract solver ID
def extract_solver_id(file_name):
    parts = file_name.split('_')
    if len(parts) > 1:
        return int(parts[1])  # Extract the solver ID
    return None

# Helper function to extract entry number
def extract_entry_number(file_name):
    try:
        return int(file_name.split('_')[0])
    except ValueError:
        return 0