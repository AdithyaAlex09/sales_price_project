import os
import zipfile

def extract_zip(zip_file, extract_folder):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

if __name__ == "__main__":
    
    zip_file_path = "D:\\sales_price_project\data\\bigmart-sales-data.zip"
    extract_folder_path = "D://sales_price_project/data"
    
    os.makedirs(extract_folder_path, exist_ok=True)
    extract_zip(zip_file_path, extract_folder_path)
