import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import pandas as pd

def l1_experiment(alpha=0.1, learning_rate=0.3, optimizer="Adam"):
    # Load dataset
    df = pd.read_csv("src/preprocessed_data/preprocessed_train.csv")

    # Split dataset into features (X) and target variable (y)
    X = df.drop('Item_Outlet_Sales', axis=1)  # Features columns
    y = df['Item_Outlet_Sales']  # Target column

    # Split the dataset into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Define the MLflow experiment name
    experiment_name = "lasso_regression"

    # Set MLflow tracking URI
    mlflow.set_tracking_uri("http://localhost:5000")

    # Set the name of the experiment
    mlflow.set_experiment(experiment_name)

    # Start MLflow run
    with mlflow.start_run(run_name="lasso_regression_run") as run:
        # Create Lasso model with L1 regularization
        model = Lasso(alpha=alpha)

        # Train the model
        model.fit(X_train, y_train)

        # Make predictions
        y_pred = model.predict(X_test)

        # Log alpha hyperparameter
        mlflow.log_param("alpha", alpha)

        # Log model metrics
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        # Log the model
        mlflow.sklearn.log_model(model, "lasso_model")

        # Log tags
        mlflow.set_tag("learning_rate", learning_rate)
        mlflow.set_tag("optimizer", optimizer)

        # Print metrics
        print("Mean Squared Error:", mse)
        print("Mean Absolute Error:", mae)
        print("R2 Score:", r2)

        # Print the run ID
        print("MLflow Run ID:", run.info.run_id)

if __name__ == "__main__":
    l1_experiment()



