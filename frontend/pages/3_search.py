import streamlit as st
import requests
import pandas as pd
# from streamlit_toggle import toggle

def search_embedding_data(embeddingType, apiToken, indexType, showData, query):
    # API endpoint URL
    api_url = "https://example.com/api/searchembeddings"  # Replace with your API endpoint URL

    # Parameters for the API request
    params = {
        "embeddingType": embeddingType,
        "apiToken": apiToken,
        "indexType": indexType,
        "showData": showData,
        "query": query,
    }

    # Make the API request
    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return []

# Streamlit app
st.title("Data Search App")

# Input fields

# company_name = st.text_input("Enter company name:")
# year_range = st.slider("Select year range:", 2014, 2023, (2015, 2018), 1)

embeddingType = st.selectbox(
    'Embedding Type',
    ('OpenAI', 'Sbert'))

st.write('You selected:', embeddingType)

apiToken = st.text_input("Enter API token:")

indexType = st.selectbox(
    'Index Type',
    ('Flat', 'HNSW'))

st.write('You selected:', indexType)

showData = st.checkbox('Show Data')
if showData:
    st.write('View the Data!')

queryOne = st.text_input("Enter query one:")
# queryTwo = st.text_input("Enter query two:")
# queryThree = st.text_input("Enter query three:")


# Search button
if st.button("Search"):
    # Perform the search
    results = search_embedding_data(embeddingType, apiToken, indexType, showData, queryOne)
    # Display results
    st.subheader("Search Results")
    if results:
        # Convert results to a DataFrame
        df = pd.DataFrame(results)
        # Display the DataFrame as a table
        st.table(df)
    else:
        st.write("No results found.")