import os

os.environ["KAGGLE_CONFIG_DIR"] = "D:\.kaggle"

from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset(dataset, download_path):
    
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset=dataset, path=download_path)

if __name__ == "__main__":
    dataset_name = 'brijbhushannanda1979/bigmart-sales-data'
    download_folder = "D://sales_price_project//data"
    
    download_kaggle_dataset(dataset_name, download_folder)


