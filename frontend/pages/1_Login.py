import os
import json
import requests
import pandas as pd
import streamlit as st

st.title('Login')

BASE_URL = os.getenv("API_URL", "http://localhost:8000/api/v1")


# @st.cache_data
def login_page():
    st.divider()

    with st.form("login", clear_on_submit=True):
        email_address = st.text_input("Email", value="", placeholder="Enter your email!", key="email_address")
        password = st.text_input("Password", type="password", value="", placeholder="Enter your password!",
                                 key="password")

        submitted = st.form_submit_button("Login")
        if submitted:
            if st.spinner("Loading..."):
                url = f"{BASE_URL}/login"

                json_payload = json.dumps({"email": email_address, "password": password})
                headers = {'Content-Type': 'application/json'}
                response = requests.request(
                    "POST", url, headers=headers, data=json_payload)

                if response.status_code == 200:
                    st.session_state['email'] = response.json()["email"]
                    st.session_state["token"] = response.json()["token"]

                    st.json(response.json())
                else:
                    st.error("Invalid Credentials!!!, Come back again")


login_page()
