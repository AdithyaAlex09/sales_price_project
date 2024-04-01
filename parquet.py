import pandas as pd

# Load your dataset into a pandas DataFrame (replace 'data.csv' with your file path)
df = pd.read_csv('D:\sales_price_project\data\Test.csv')

# Specify the full path where you want to save the Parquet file
parquet_file_path = 'D:/sales_price_project/data/data.parquet'

# Convert the DataFrame to a Parquet file
df.to_parquet(parquet_file_path)
