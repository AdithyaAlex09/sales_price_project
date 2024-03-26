import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.base import BaseEstimator

df1 = pd.read_csv("D:\sales_price_project\data\Train.csv")

X = df1.drop('Item_Outlet_Sales',axis=True)
y = df1['Item_Outlet_Sales']

class RegressionMetrics(BaseEstimator):
    def __init__(self, model):
        self.model = model

    def fit(self, X, y):
        self.model.fit(X, y)
        return self

    def predict(self, X):
        return self.model.predict(X)

    def calculate_metrics(self, X_train, X_test, y_train, y_test):
        self.fit(X_train, y_train)
        y_pred = self.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        return mse, mae, r2

def main():
    # Example usage
    from sklearn.linear_model import LinearRegression
    from sklearn.datasets import load_diabetes

    # Load the diabetes dataset (as an example)
    diabetes = load_diabetes()
    X = diabetes.data
    y = diabetes.target

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize a LinearRegression model
    model = LinearRegression()

    # Initialize the RegressionMetrics class with the model
    regressor = RegressionMetrics(model)

    # Calculate regression metrics
    mse, mae, r2 = regressor.calculate_metrics(X_train, X_test, y_train, y_test)

    # Print regression metrics
    print("Mean Squared Error (MSE):", mse)
    print("Mean Absolute Error (MAE):", mae)
    print("R-squared (R2) Score:", r2)

if __name__ == "__main__":
    main()
