import os
import pandas as pd
from sklearn.linear_model import Lasso, LinearRegression

def train_lasso_model(x_train, y_train, alpha=0.1):
    Lasso_model = Lasso(alpha=alpha)
    Lasso_model.fit(x_train, y_train)
    return Lasso_model

def train_linear_model(x_train, y_train):
    linear_model = LinearRegression()
    linear_model.fit(x_train, y_train)
    return linear_model

def save_lasso_predictions(Lasso_model, test_df, save_folder):
    predictions = Lasso_model.predict(test_df)
    predictions_df = pd.DataFrame(predictions, columns=['Prediction'])
    save_path = os.path.join(save_folder, 'Lassopredictions.csv')
    predictions_df.to_csv(save_path, index=False)
    print("Lasso predictions saved to:", save_path)
    
def save_linear_predictions(linear_model, test_df, save_folder):
    predictions = linear_model.predict(test_df)
    predictions_df = pd.DataFrame(predictions, columns=['Prediction'])
    save_path = os.path.join(save_folder, 'LinearPredictions.csv')
    predictions_df.to_csv(save_path, index=False)
    print("Linear predictions saved to:", save_path)
   

if __name__ == "__main__":
    
    train_df=pd.read_csv("D:\sales_price_project\src\preprocessed_data\preprocessed_train.csv")
    test_df=pd.read_csv("D:\sales_price_project\src\preprocessed_data\preprocessed_test.csv")
    
    x_train = train_df.drop('Item_Outlet_Sales', axis=1) #features_columns
    y_train = train_df['Item_Outlet_Sales'] #target_column
    
    Lasso_model = train_lasso_model(x_train, y_train)
    linear_model = train_linear_model(x_train, y_train)
    
    save_folder = "D:/sales_price_project/src//predictions_data"
    
    save_lasso_predictions(Lasso_model, test_df, save_folder)
    save_linear_predictions(linear_model, test_df, save_folder)




