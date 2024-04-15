import sys
sys.path.append("D:/sales_price_project/src")

import os
import joblib
from model_training.lassomodel import train_lasso_model
from model_training.linearmodel import train_linear_regression_model

def save_models(save_folder):
    os.makedirs(save_folder, exist_ok=True)
    lasso_model_path = os.path.join(save_folder, 'Lasso_model.pkl')
    linear_model_path = os.path.join(save_folder, 'Linear_model.pkl')
    joblib.dump(train_lasso_model, lasso_model_path)  # Call the function
    joblib.dump(train_linear_regression_model, linear_model_path)
    print("Models saved to:", save_folder)

if __name__ == "__main__":
    save_folder = "D:/sales_price_project/src/final_models"
    save_models(save_folder)

    

