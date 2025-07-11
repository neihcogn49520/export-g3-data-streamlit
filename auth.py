import streamlit as st
import requests
from configparser import ConfigParser

# === Load credentials from config file ===
config = ConfigParser()
config.read("config/default.ini")
USERNAME = st.secrets["DEFAULT"]["USERNAME"]
PASSWORD = st.secrets["DEFAULT"]["PASSWORD"]
AUTH_URL = st.secrets["DEFAULT"]["authUrl"] # Replace with your real token endpoint
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,vi;q=0.8",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://socialheat.younetmedia.com",
    "referer": "https://socialheat.younetmedia.com/",
    "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
}

def get_token():
    payload = {
        "email": USERNAME,
        "password": PASSWORD,
        "strategy": "local"
    }

    try:
        response = requests.post(AUTH_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        data = response.json()
        return data.get("accessToken")  # correct field name from API
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Failed to get token: {e}")
        return None
