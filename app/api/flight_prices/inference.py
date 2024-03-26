import numpy as np
import pandas as pd
from flight_prices import FEATURE_COLUMNS, MODEL_PATH, ENCODER_PATH, COLUMNS_ID
from flight_prices.preprocess import encode_categorical_features
from flight_prices.preprocess import load_model_and_encoder


def make_predictions(input_data: pd.DataFrame) -> np.ndarray:
    model, encoder = load_model_and_encoder(MODEL_PATH, ENCODER_PATH)
    feature = input_data[FEATURE_COLUMNS]
    encoded_feature = encode_categorical_features(feature, encoder, False)
    predictions = model.predict(encoded_feature)
    predictions_flat = predictions.flatten()
    id_series = input_data[COLUMNS_ID].squeeze()
    submission_df = pd.DataFrame({'Id': id_series,
                                  'SalesPrice': predictions_flat})
    return submission_df
