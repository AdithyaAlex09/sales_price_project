import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

train_df = pd.read_csv('D:\sales_price_project\src\preprocessed_data\preprocessed_train.csv')

x = train_df.drop('Item_Outlet_Sales', axis=1) #features_columns
y = train_df['Item_Outlet_Sales'] #target_column

# Assuming x_train and y_train are already defined
x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.2, random_state=42)
# Split the data into training and testing sets
x_train_split, x_test_split, y_train_split, y_test_split = train_test_split(x_train, y_train, test_size=0.2, random_state=42)

# Train the Linear Regression model
LinearRegression_model = LinearRegression()
LinearRegression_model.fit(x_train_split, y_train_split)

# Calculate train score
train_score = LinearRegression_model.score(x_train_split, y_train_split)
print("Linear Regression Train Score:", train_score)

# Calculate test score
test_score = LinearRegression_model.score(x_test_split, y_test_split)
print("Linear Regression Test Score:", test_score)

# Make predictions on both train and test sets
y_train_pred = LinearRegression_model.predict(x_train_split)
y_test_pred = LinearRegression_model.predict(x_test_split)

# Calculate evaluation metrics for training set
mse_train = mean_squared_error(y_train_split, y_train_pred)
mae_train = mean_absolute_error(y_train_split, y_train_pred)
r2_train = r2_score(y_train_split, y_train_pred)

print("Training Set Metrics:")
print("Mean Squared Error (MSE):", mse_train)
print("Mean Absolute Error (MAE):", mae_train)
print("Coefficient of Determination (R^2):", r2_train)

# Calculate evaluation metrics for test set
mse_test = mean_squared_error(y_test_split, y_test_pred)
mae_test = mean_absolute_error(y_test_split, y_test_pred)
r2_test = r2_score(y_test_split, y_test_pred)

print("Test Set Metrics:")
print("Mean Squared Error (MSE):", mse_test)
print("Mean Absolute Error (MAE):", mae_test)
print("Coefficient of Determination (R^2):", r2_test)
