import os 
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler



def find_categorical_numerical_columns(df):
    categorical_features = df.select_dtypes(include=['object']).columns
    numerical_features = df.select_dtypes(include=['int64', 'float64']).columns
    return categorical_features, numerical_features

def fill_missing_values(df):
    df['Item_Weight'] = np.where(df['Item_Weight'].isna(), df['Item_Weight'].median(skipna=True), df['Item_Weight'])
    df['Outlet_Size'] = np.where(df['Outlet_Size'].isna(), df['Outlet_Size'].mode()[0], df['Outlet_Size'])
    return df

def clean_item_fat_content(df):
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('low fat', 'Low Fat')
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('LF', 'Low Fat')
    df['Item_Fat_Content'] = df['Item_Fat_Content'].replace('reg', 'Regular')
    return df

def label_encode_categorical_features(train_df, test_df, categorical_columns):
    label_encoder = LabelEncoder()
    for feature in categorical_columns:
        train_df[feature] = label_encoder.fit_transform(train_df[feature])
        test_df[feature] = label_encoder.transform(test_df[feature])
    return train_df, test_df

def add_item_identifier_categories_column(df):
    df['Item_Identifier_Categories'] = df['Item_Identifier'].str[:2]
    return df

def one_hot_encode(df, columns):
    return pd.get_dummies(df, columns=columns, drop_first=True)

def drop_column(df, column_name):
    df.drop(labels=[column_name], axis=1, inplace=True)
    return df


def main():
    
    train_df=pd.read_csv("D:\sales_price_project\data\Train.csv")
    test_df=pd.read_csv("D:\sales_price_project\data\Test.csv")
    
    categorical_features, numerical_features = find_categorical_numerical_columns(train_df)
    print("Categorical columns:", categorical_features)
    print("Numerical columns:", numerical_features)

    train_df = fill_missing_values(train_df)
    test_df = fill_missing_values(test_df)
    
    train_df = clean_item_fat_content(train_df)
    test_df = clean_item_fat_content(test_df)
    
    print("Item_Fat_Content_Unique_Values :",train_df.Item_Fat_Content.unique())
    print("Outlet_Size_Unique_Values :",train_df.Outlet_Size.unique())
    print("Outlet_Type_Unique_Values :",train_df.Outlet_Type.unique())
    print("Outlet_Location_Type_Unique_Values :",train_df.Outlet_Location_Type.unique())
    
    
    categorical_columns = ['Item_Fat_Content', 'Outlet_Size', 'Outlet_Type', 'Outlet_Location_Type']
    train_df, test_df = label_encode_categorical_features(train_df, test_df, categorical_columns)
    
    for feature in categorical_columns:
        
        print(feature, "unique values in train:", train_df[feature].unique())
        print(feature, "unique values in test:", test_df[feature].unique())
        
    train_df = add_item_identifier_categories_column(train_df)
    test_df = add_item_identifier_categories_column(test_df)
    
    train_columns = ['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier']
    test_columns = ['Item_Type', 'Item_Identifier_Categories', 'Outlet_Identifier']

    train_df = one_hot_encode(train_df, train_columns)
    test_df = one_hot_encode(test_df, test_columns)
    
    train_df = drop_column(train_df, 'Item_Identifier')
    test_df = drop_column(test_df, 'Item_Identifier')
    print("shape_of_train_df :",train_df.shape)
    
    #saving preprocessed data
    folder = "D://sales_price_project/src/preprocessed_data"
    
    train_save_path = os.path.join(folder,'preprocessed_train.csv')
    train_df.to_csv(train_save_path, index=False)
    
    test_save_path = os.path.join(folder,'preprocessed_test.csv')
    test_df.to_csv(test_save_path, index=False)
    
    
if __name__ == "__main__":
    main()