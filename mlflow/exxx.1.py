import mlflow
import numpy as np 
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os

def mlflow_experiment1():
    # Set MLflow tracking URI
    mlflow.set_tracking_uri("http://localhost:5000")

    # Set the name of the experiment
    experiment_name = "PricePredictionExperiment0.2"

    # Set the name of the experiment
    mlflow.set_experiment(experiment_name)

    # Start a new MLflow run
    with mlflow.start_run(run_name=experiment_name) as run:
        # Load dataset
        df = pd.read_csv("C:/Users/Admin/Documents/ISCS/sales_price_project/sales_price_project/src/preprocessed_data/preprocessed_train.csv")

        # Split dataset into features (X) and target variable (y)
        x = df.drop('Item_Outlet_Sales', axis=1)  # Features columns
        y = df['Item_Outlet_Sales']  # Target column

        # Split dataset into training and testing sets
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

        # Train Lasso regression model
        lasso_model = Lasso(alpha=0.1, max_iter=1000)  # Adjust alpha and max_iter as needed
        lasso_model.fit(x_train, y_train)

        # Make predictions on the test set
        y_pred = lasso_model.predict(x_test)

        # Evaluate model performance
        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # Log model parameters and metrics with MLflow
        mlflow.log_params(lasso_model.get_params())
        mlflow.log_metric("MAE", mae)
        mlflow.log_metric("MSE", mse)
        mlflow.log_metric("R2", r2)

        # Save model
        mlflow.sklearn.log_model(lasso_model, "model")

        # Log artifacts (e.g., model file)
        artifact_path = "mlflow/artifacts"
        os.makedirs(artifact_path, exist_ok=True)
        mlflow.log_artifacts(artifact_path)

if __name__ == "__main__":
    mlflow_experiment1()
