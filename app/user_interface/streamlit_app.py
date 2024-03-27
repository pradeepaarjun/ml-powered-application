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
    page = st.sidebar.selectbox("Select how you would like to input", ["Manual Input", "Upload CSV"])

    if page == "Manual Input":
        show_manual_input_page()
    elif page == "Upload CSV":
        show_upload_csv_page()

def show_manual_input_page():
    st.write("## Enter Flight Details Manually")
    airline = st.selectbox("Airline",["SpiceJet", "AirAsia", "Vistara", "GO_FIRST", "Indigo", "Air_India"])
    class_ = st.selectbox("Class", ["Economy", "Business", "First"])
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

def predict_price(airline, class_, duration, days_left):
    data = {
        "airline": airline,
        "Class": class_,
        "duration": duration,
        "days_left": days_left
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
            st.write(f"Response status code: {response.status_code}")
            st.write(f"Response content: {response.content}")
    except Exception as e:
        st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
