import os
import pandas as pd
from sklearn.linear_model import Lasso

def train_lasso_model(x_train, y_train, alpha=0.1):
    Lasso_model = Lasso(alpha=alpha)
    Lasso_model.fit(x_train, y_train)
    return Lasso_model

def save_predictions(Lasso_model, test_df, save_folder):
    predictions = Lasso_model.predict(test_df)
    predictions_df = pd.DataFrame(predictions, columns=['Prediction'])
    save_path = os.path.join(save_folder, 'Lassopredictions.csv')
    predictions_df.to_csv(save_path, index=False)
    print("Predictions saved to:", save_path)
   

if __name__ == "__main__":
    
    train_df=pd.read_csv("D:\sales_price_project\preprocessed_data\preprocessed_train.csv")
    test_df=pd.read_csv("D:\sales_price_project\preprocessed_data\preprocessed_test.csv")
    
    x_train = train_df.drop('Item_Outlet_Sales', axis=1) #features_columns
    y_train = train_df['Item_Outlet_Sales'] #target_column
    
    Lasso_model = train_lasso_model(x_train, y_train)
    
    save_folder = "D:/sales_price_project/predictions_data"
    save_path = os.path.join(save_folder, 'Lassopredictions.csv')
    
    save_predictions(Lasso_model, test_df, save_folder)
    print("Predictions saved to:", save_path)
      


