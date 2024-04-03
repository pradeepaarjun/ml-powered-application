import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from flight_prices import FEATURE_COLUMNS, TARGET_VARIABLE
from flight_prices.preprocess import encode_categorical_features
from flight_prices.preprocess import train_model
from flight_prices.preprocess import compute_r2

onehot_encoder = OneHotEncoder()


def build_model(data: pd.DataFrame) -> dict[str, str]:
    feature, target = data[FEATURE_COLUMNS], data[TARGET_VARIABLE]
    X_train, X_test, y_train, y_test = train_test_split(
        feature, target, test_size=0.2, random_state=42)
    encoded_train = encode_categorical_features(X_train, onehot_encoder, True)
    encoded_test = encode_categorical_features(X_test, onehot_encoder, True)
    model = train_model(encoded_train, y_train)
    y_pred = model.predict(encoded_test)
    r2=compute_r2(y_test, y_pred)
    model_performances = {'R2': r2,}
    return model_performances
