COLUMNS_ID = ['Unnamed: 0']
COLUMNS_TO_ENCODE = ['airline', 'class']
CATEGORICAL_COLUMNS = ['airline', 'class']
CONTINUOUS_COLUMNS = ['duration', 'days_left']
FEATURE_COLUMNS = CATEGORICAL_COLUMNS + CONTINUOUS_COLUMNS
TARGET_VARIABLE = ['price']
MODEL_PATH = '../models/model.joblib'
ENCODER_PATH = '../models/encoder.joblib'