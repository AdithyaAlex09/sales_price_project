import os 
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer


def find_categorical_numerical_columns(df):
    categorical_features = df.select_dtypes(include=['object']).columns
    numerical_features = df.select_dtypes(include=['int64', 'float64']).columns
    return categorical_features, numerical_features


def fill_missing_values(df):
    df['Item_Weight'] = np.where(df['Item_Weight'].isna(), df['Item_Weight'].median(skipna=True), df['Item_Weight'])
    df['Outlet_Size'] = np.where(df['Outlet_Size'].isna(), df['Outlet_Size'].mode()[0], df['Outlet_Size'])
    return df


def main():
    
    train_df=pd.read_csv("D:\sales_price_project\data\Train.csv")
    test_df=pd.read_csv("D:\sales_price_project\data\Test.csv")
    
    categorical_features, numerical_features = find_categorical_numerical_columns(train_df)
    print("Categorical columns:", categorical_features)
    print("Numerical columns:", numerical_features)

    train_df = fill_missing_values(train_df)
    test_df = fill_missing_values(test_df)

    folder = "D://sales_price_project/preprocessed_data"
    
    train_save_path = os.path.join(folder,'preprocessed_train.csv')
    train_df.to_csv(train_save_path, index=False)
    
    test_save_path = os.path.join(folder,'preprocessed_test.csv')
    test_df.to_csv(test_save_path, index=False)
    
    
if __name__ == "__main__":
    main()










