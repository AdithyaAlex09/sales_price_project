import os
import zipfile

def extract_zip(zip_file, extract_folder):
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

if __name__ == "__main__":
    # Specify the path to the zip file
    zip_file_path = "D:\\sales_price_project\data\\bigmart-sales-data.zip"

    # Specify the path to the folder where you want to extract the contents
    extract_folder_path = "D://sales_price_project/data"

    # Ensure the extract folder exists, or create it if it doesn't
    os.makedirs(extract_folder_path, exist_ok=True)

    # Call the function to extract the zip file
    extract_zip(zip_file_path, extract_folder_path)
