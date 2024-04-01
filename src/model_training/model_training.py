import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Lasso
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
from sklearn.svm import SVR


def train_linear_regression_model(x_train, y_train):
    LinearRegression_model = LinearRegression()
    LinearRegression_model.fit(x_train, y_train)
    train_score = LinearRegression_model.score(x_train, y_train)
    return train_score

def train_decision_tree_model(x_train, y_train):
    DecisionTree_model = DecisionTreeRegressor()
    DecisionTree_model.fit(x_train, y_train)
    train_score = DecisionTree_model.score(x_train, y_train)
    return train_score

def train_random_forest_model(x_train, y_train):
    RandomForest_model = RandomForestRegressor()
    RandomForest_model.fit(x_train, y_train)
    train_score = RandomForest_model.score(x_train, y_train)
    return train_score

def train_lasso_model(x_train, y_train, alpha=0.1,max_iter=1000):
    Lasso_model = Lasso(alpha=alpha, max_iter=max_iter)
    Lasso_model.fit(x_train, y_train)
    train_score = Lasso_model.score(x_train, y_train)
    return train_score

def train_xgboost_model(x_train, y_train):
    XGB_model = xgb.XGBRegressor()
    XGB_model.fit(x_train, y_train)
    train_score = XGB_model.score(x_train, y_train)
    return train_score

def train_svm_model(x_train, y_train):
    SVM_model = SVR()
    SVM_model.fit(x_train, y_train)
    train_score = SVM_model.score(x_train, y_train)
    return train_score


#saving_best_model
def predict_and_save(SVM_model, test_df, save_folder, filename='SVMpredictions.csv'):
    # Make predictions
    predictions = SVM_model.predict(test_df)
    
    # Create DataFrame from predictions
    predictions_df = pd.DataFrame(predictions, columns=['Prediction'])
    
    # Create save path
    save_path = os.path.join(save_folder, filename)
    
    # Save predictions to CSV
    predictions_df.to_csv(save_path, index=False)





if __name__ == "__main__":
    
    train_df=pd.read_csv("D:\sales_price_project\src\preprocessed_data\preprocessed_train.csv")
    test_df=pd.read_csv("D:\sales_price_project\src\preprocessed_data\preprocessed_test.csv")
    
    x_train = train_df.drop('Item_Outlet_Sales', axis=1) #features_columns
    y_train = train_df['Item_Outlet_Sales'] #target_column
    
    train_score = train_linear_regression_model(x_train, y_train)
    print("LinearRegression Train Score:", train_score)

    train_score = train_decision_tree_model(x_train, y_train)
    print("DecisionTree Train Score:", train_score)

    train_score = train_random_forest_model(x_train, y_train)
    print("RandomForest Train Score:", train_score)

    train_score = train_lasso_model(x_train, y_train)
    print("LassoRegression Train Score:", train_score)
    
    train_score = train_xgboost_model(x_train, y_train)
    print("XGBoost Train Score:", train_score)
    
    train_score = train_svm_model(x_train, y_train)
    print("SVM Train Score:", train_score)




if __name__ == "__main__":
    # Example usage
    SVM_model = SVR()  # Replace with your trained SVM model
    test_df = ...  # Replace with your test DataFrame
    folder = "D://sales_price_project/predictions_data"
    
    predict_and_save(SVM_model, test_df, folder)





     
