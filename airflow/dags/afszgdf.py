import json
from sklearn.linear_model import Lasso

# Function to load the trained model from MinIO
def load_model():
    try:
        # Download model file from MinIO
        response = s3.get_object(Bucket=MODEL_BUCKET_NAME, Key=MODEL_OBJECT_NAME)
        model_bytes = response['Body'].read()
        
        # Deserialize the model dictionary from JSON
        model_dict = json.loads(model_bytes)
        
        # Reconstruct the Lasso model
        lasso_regression_model = Lasso()
        lasso_regression_model.coef_ = np.array(model_dict["coefficients"])
        lasso_regression_model.intercept_ = model_dict["intercept"]
        
        return lasso_regression_model
    except ClientError as e:
        logging.error(f"Error fetching model from MinIO: {e}")
        raise



def load_model():
    try:
        # Download model file from MinIO
        response = s3.get_object(Bucket=MODEL_BUCKET_NAME, Key=MODEL_OBJECT_NAME)
        model_bytes = response['Body'].read()
        
        # Deserialize the model dictionary from JSON
        model_dict = json.loads(model_bytes)
        
        # Reconstruct the Lasso model
        linear_regression_model = LinearRegression()
        linear_regression_model.coef_ = np.array(model_dict["coefficients"])
        linear_regression_model.intercept_ = model_dict["intercept"]
        
        return linear_regression_model
    except ClientError as e:
        logging.error(f"Error fetching model from MinIO: {e}")
        raise