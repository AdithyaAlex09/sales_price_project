import os

os.environ["KAGGLE_CONFIG_DIR"] = "D:\.kaggle"

from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset(dataset, download_path):
  
    # Instantiate the Kaggle API client
    api = KaggleApi()

    # Authenticate with your Kaggle credentials
    api.authenticate()

    # Download the dataset
    api.dataset_download_files(dataset=dataset, path=download_path)

if __name__ == "__main__":
    # Specify the dataset to download
    dataset_name = 'brijbhushannanda1979/bigmart-sales-data'

    # Specify the folder path where you want to save the downloaded zip file
    download_folder = "D://sales_price_project//data"

    # Call the function to download the dataset
    download_kaggle_dataset(dataset_name, download_folder)


