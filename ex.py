import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
import xgboost as xgb
from sklearn.svm import SVR
from sklearn.metrics import r2_score

def train_models(x_train, y_train):
    models = {}

    # Train Linear Regression model
    LinearRegression_model = LinearRegression()
    LinearRegression_model.fit(x_train, y_train)
    train_score = cross_val_score(LinearRegression_model, x_train, y_train, cv=5).mean()
    models['Linear Regression'] = (LinearRegression_model, train_score)

    # Train Lasso model
    Lasso_model = Lasso()
    Lasso_model.fit(x_train, y_train)
    train_score = cross_val_score(Lasso_model, x_train, y_train, cv=5).mean()
    models['Lasso'] = (Lasso_model, train_score)

    # Train Decision Tree model with hyperparameter tuning
    DecisionTree_model = DecisionTreeRegressor()
    param_grid = {'max_depth': [3, 5, 7, 10]}
    grid_search = GridSearchCV(DecisionTree_model, param_grid, cv=5, scoring='r2')
    grid_search.fit(x_train, y_train)
    best_dt_model = grid_search.best_estimator_
    train_score = cross_val_score(best_dt_model, x_train, y_train, cv=5).mean()
    models['Decision Tree'] = (best_dt_model, train_score)

    # Train Random Forest model with hyperparameter tuning
    RandomForest_model = RandomForestRegressor()
    param_grid = {'n_estimators': [50, 100, 150], 'max_depth': [5, 10, 15]}
    grid_search = GridSearchCV(RandomForest_model, param_grid, cv=5, scoring='r2')
    grid_search.fit(x_train, y_train)
    best_rf_model = grid_search.best_estimator_
    train_score = cross_val_score(best_rf_model, x_train, y_train, cv=5).mean()
    models['Random Forest'] = (best_rf_model, train_score)

    # Train XGBoost model with hyperparameter tuning
    XGB_model = xgb.XGBRegressor()
    param_grid = {'n_estimators': [50, 100, 150], 'max_depth': [3, 5, 7]}
    grid_search = GridSearchCV(XGB_model, param_grid, cv=5, scoring='r2')
    grid_search.fit(x_train, y_train)
    best_xgb_model = grid_search.best_estimator_
    train_score = cross_val_score(best_xgb_model, x_train, y_train, cv=5).mean()
    models['XGBoost'] = (best_xgb_model, train_score)

    # Train SVM model
    SVM_model = SVR()
    SVM_model.fit(x_train, y_train)
    train_score = cross_val_score(SVM_model, x_train, y_train, cv=5).mean()
    models['SVM'] = (SVM_model, train_score)

    return models

def save_best_model(models, save_folder):
    best_model_name, best_model_score = max(models.items(), key=lambda x: x[1][1])
    best_model, _ = best_model_score
    save_path = os.path.join(save_folder, f'{best_model_name}_model.pkl')
    joblib.dump(best_model, save_path)
    print(f"Best model ({best_model_name}) saved successfully at:", save_path)

if __name__ == "__main__":
    train_df = pd.read_csv("D:/sales_price_project/src/preprocessed_data/preprocessed_train.csv")
    test_df = pd.read_csv("D:/sales_price_project/src/preprocessed_data/preprocessed_test.csv")
    
    x_train = train_df.drop('Item_Outlet_Sales', axis=1)
    y_train = train_df['Item_Outlet_Sales']
    
    models = train_models(x_train, y_train)
    save_folder = "D:/sales_price_project/final_model"
    save_best_model(models, save_folder)


