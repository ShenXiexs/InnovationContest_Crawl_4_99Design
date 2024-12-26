import os
from PIL import Image

def convert_png_to_jpg(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.png'):
                png_path = os.path.join(root, file)
                jpg_path = os.path.join(root, file.replace('.png', '.jpg'))
                try:
                    with Image.open(png_path) as img:
                        rgb_img = img.convert('RGB')
                        rgb_img.save(jpg_path, 'JPEG')
                    os.remove(png_path)  # Optionally remove the original PNG file
                    print(f"Converted: {png_path} to {jpg_path}")
                except Exception as e:
                    print(f"Failed to convert {png_path}: {str(e)}")

if __name__ == "__main__":
    folder_path = "/Users/samxie/Research/CrowdImageComp/99design/OngoingCrawl/Image_99Design/ContestImage/OngoingFrom241004"
    convert_png_to_jpg(folder_path)
