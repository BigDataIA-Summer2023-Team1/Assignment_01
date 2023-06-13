import os
import streamlit as st
import requests
import json
import pandas as pd


def search_data(year_range, company_name):
    # API endpoint URL
    BASE_URL = os.getenv("API_URL", "http://localhost:8000/api/v1")

    # Make the API request
    url = f"{BASE_URL}/fetch-companies-data"

    json_payload = json.dumps({"company": company_name, "startYr": year_range[0], "endYr": year_range[1]})
    headers = {'Content-Type': 'application/json'}
    response = requests.request("GET", url, headers=headers, data=json_payload)

    if response.status_code == 200:
        data = response.json()

        return data
    else:
        return []

# Streamlit app
st.title("Data Search App")

# Input fields

company_name = st.text_input("Enter company name:")
year_range = st.slider("Select year range:", 2014, 2023, (2015, 2018), 1)

# Search button
if st.button("Search"):
    # Perform the search
    results = search_data(year_range, company_name)

    # Display results
    st.subheader("Search Results")
    if results:
        # Convert results to a DataFrame
        df = pd.DataFrame(results)
        # Display the DataFrame as a table
        st.json(results)

        # st.table(df)
    else:
        st.write("No results found.")
