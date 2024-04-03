from sklearn.linear_model import LinearRegression
import numpy as np
from sklearn.metrics import r2_score
import joblib
from flight_prices import COLUMNS_TO_ENCODE, ENCODER_PATH, MODEL_PATH


def encode_categorical_features(X, encoder, flag):
    if flag:
        encoder.fit(X[COLUMNS_TO_ENCODE])
        joblib.dump(encoder, ENCODER_PATH)
        encoded_data = encoder.transform(X[COLUMNS_TO_ENCODE])
        encoded_data = np.hstack((encoded_data.toarray(),
                                  X.drop(COLUMNS_TO_ENCODE, axis=1)))
    else:
        encoded_data = encoder.transform(X[COLUMNS_TO_ENCODE])
        encoded_data = np.hstack((encoded_data.toarray(),
                                  X.drop(COLUMNS_TO_ENCODE, axis=1)))
    return encoded_data


def train_model(X, y):
    model = LinearRegression()
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)
    return model


def compute_r2(y_test: np.ndarray,
                  y_pred: np.ndarray, precision: int = 2) -> float:
    r2 = np.sqrt(r2_score(y_test, y_pred))
    return round(r2, precision)


def load_model_and_encoder(model_path, encoder_path):
    model = joblib.load(model_path)
    encoder = joblib.load(encoder_path)
    return model, encoder
