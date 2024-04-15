# linearmodel.py
import numpy as np 
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def train_test_split_data(x, y, test_size=0.2, random_state=42):
    
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, random_state=random_state)
    return x_train, x_test, y_train, y_test

def train_linear_regression_model(x_train, y_train):
 
    linear_regression_model = LinearRegression()
    linear_regression_model.fit(x_train, y_train)
    return linear_regression_model

def evaluate_model(model, x_train, y_train, x_test, y_test):
    
    y_train_pred = model.predict(x_train)
    y_test_pred = model.predict(x_test)
    
    train_mae = mean_absolute_error(y_train, y_train_pred)
    test_mae = mean_absolute_error(y_test, y_test_pred)
    
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)
    
    train_rmse = np.sqrt(train_mse)
    test_rmse = np.sqrt(test_mse)
    
    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    
    print("Train MAE:", train_mae)
    print("Test MAE:", test_mae)
    
    print("Train MSE:", train_mse)
    print("Test MSE:", test_mse)
    
    print("Train RMSE:", train_rmse)
    print("Test RMSE:", test_rmse)
    
    print("Train R2 Score:", train_r2)
    print("Test R2 Score:", test_r2)
    
    return train_r2, test_r2

def load_data():
    
    train_df = pd.read_csv("D:\sales_price_project\src\preprocessed_data\preprocessed_train.csv")
    
    x = train_df.drop('Item_Outlet_Sales', axis=1)  # Features columns
    y = train_df['Item_Outlet_Sales']  # Target column
    
    x_train, x_test, y_train, y_test = train_test_split_data(x, y)
    
    model = train_linear_regression_model(x_train, y_train)
    
    train_score, test_score = evaluate_model(model, x_train, y_train, x_test, y_test)
    
    print("Linear Regression Train Score:", train_score)
    print("Linear Regression Test Score:", test_score)

if __name__ == "__main__":
    load_data()


