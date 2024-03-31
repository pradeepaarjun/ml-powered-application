from datetime import datetime
import streamlit as st
import pandas as pd
import requests

def main():
    html_temp = """
    <div style="background:#025246 ;padding:10px">
    <h2 style="color:white;text-align:center;">Flight Price Prediction App </h2>
    </div>
    """
    st.markdown(html_temp, unsafe_allow_html = True)
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select how you would like to input", ["Manual Input", "Upload CSV", "Past Predictions"])

    if page == "Manual Input":
        show_manual_input_page()
    elif page == "Upload CSV":
        show_upload_csv_page()
    elif page == "Past Predictions":
        show_past_predictions_page()

def show_manual_input_page():
    st.write("## Enter Flight Details Manually")
    airline = st.selectbox("Airline",["SpiceJet", "AirAsia", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
    class_ = st.selectbox("Class", ["Economy", "Business"])
    duration = st.number_input("Duration (hours)")
    days_left = st.number_input("Days Left to Departure")

    if st.button("Predict Price"):
        predict_price(airline, class_, duration, days_left)

def show_upload_csv_page():
    st.write("## Upload Flight Details as CSV")
    csv_file = st.file_uploader("Upload CSV", type=["csv"])

    if csv_file is not None:
        df = pd.read_csv(csv_file)
        st.write("### Uploaded Flight Details")
        st.write(df)

        if st.button("Predict Prices"):
            predict_prices_from_csv(df)

def show_past_predictions_page():
    st.write("## Past Predictions")
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")
    prediction_source = st.selectbox("Prediction Source", ["webapp", "scheduled predictions", "all"])

    if st.button("Retrieve Predictions"):
        retrieve_past_predictions(start_date, end_date, prediction_source)


def predict_price(airline, class_, duration, days_left):
    data = {
        "airline": airline,
        "Class": class_,
        "duration": duration,
        "days_left": days_left,
        "prediction_source": "webapp"
    }

    response = requests.post("http://127.0.0.1:8000/flights/predict-price/", json=data)

    if response.status_code == 200:
        predicted_price = response.json()["predicted_price"]
        st.write(f"Predicted Price: {predicted_price}")
    else:
        st.error("Failed to get prediction. Please check your inputs and try again.")

def predict_prices_from_csv(df):
    try:
        csv_data = df.to_csv(index=False)
        response = requests.post("http://127.0.0.1:8000/flights/predict-price/csv/", files={"csv_file": ("data.csv", csv_data)})
        if response.status_code == 200:
            predicted_prices = response.json()["predicted_prices"]
            df["predicted_price"] = predicted_prices
            st.write("### Predicted Prices")
            st.write(df)
        else:
            st.error("Failed to get predictions. Please check your CSV data and try again.")
    except Exception as e:
        st.error(f"An error occurred: {e}")

def retrieve_past_predictions(start_date, end_date, prediction_source):    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    data = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "prediction_source": prediction_source
    }
    response = requests.post("http://127.0.0.1:8000/past-predictions/", json=data)

    if response.status_code == 200:
        past_predictions = response.json()["past_predictions"]
        if not past_predictions:
            st.write("No past predictions available.")
        else:
            df = pd.DataFrame(past_predictions)
            df = df.rename(columns={'class_': 'class'})
            desired_column_order = ['airline', 'class','days_left', 'duration', 'predicted_price', 'prediction_source']
            df = df[desired_column_order]
            st.write("### Past Predictions")
            st.write(df)
    else:
        st.error("Failed to retrieve past predictions. Please try again.")
if __name__ == "__main__":
    main()
