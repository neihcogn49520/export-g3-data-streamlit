import streamlit as st
import requests
import gspread
from datetime import datetime, timedelta, date
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
from gspread_dataframe import set_with_dataframe
from configparser import ConfigParser
from export_total_buzz import export_total_buzz
from export_top_sources import export_top_sources
from export_top_posts import export_top_posts

# === Load credentials from config file ===
config = ConfigParser()
config.read("config/default.ini")

USERNAME = config.get("DEFAULT", "USERNAME")
PASSWORD = config.get("DEFAULT", "PASSWORD")
API_ENDPOINT = config.get("DEFAULT", "apiBaseUrl") + "/batch"
AUTH_TOKEN = config.get("DEFAULT", "AUTH_TOKEN")

# === Google Sheets Auth ===
@st.cache_resource
def connect_to_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    return client

client = connect_to_gsheet()

st.title("🛠️ Data Export Tool")

sheet_url = st.text_input("🔗 Enter Google Sheet URL")
default_range = (date.today() - timedelta(days=7), date.today())
date_range = st.date_input("Select Date Range", default_range, format="YYYY-MM-DD")

# Validate sheet URL
if sheet_url:
    if not sheet_url.startswith("https://docs.google.com/spreadsheets/d/"):
        st.error("❗ Invalid Google Sheet URL. Please enter a valid URL.")
        st.stop()
    if "d/" not in sheet_url or "/edit" not in sheet_url:
        st.error("❗ This sheet cannot edit. Please ensure it has edit or update permission for exporting data to new sheet.")
        st.stop()

# Buttons with conditional disabling
spacer1, col1, col2, col3, spacer2 = st.columns([2, 4, 4, 4, 2]) 

with col1:
    export_buzz_button = st.button("📊 Export Total Buzz")

with col2:
    export_sources_button = st.button("📡 Export Top 50 Sources")

with col3:
    export_posts_button = st.button("📮 Export Top 10 Posts")


if sheet_url and (export_buzz_button or export_sources_button or export_posts_button):
    try:
        sheet_id = sheet_url.split("/d/")[1].split("/")[0]
        sheet = client.open_by_key(sheet_id)

        worksheet = sheet.worksheet("Query")
        all_data = worksheet.get_all_values()
        df_all = pd.DataFrame(all_data)
    except Exception as e:
        st.error(f"❌ Failed to load Google Sheet: {e}")
        st.stop()

    try:
        # Detect split index by row that contains "LAYER" in column A (start of 2nd table)
        split_index = df_all[df_all[0] == "LAYER"].index[0]

        # Extract tables
        df_topics = df_all.iloc[1:split_index].copy()
        df_params = df_all.iloc[split_index+1:].copy()

        # Set headers
        df_topics.columns = df_all.iloc[0]         # row 0 = header of topic table
        df_params.columns = df_all.iloc[split_index]  # split_index = header of parameter table

        # Clean up
        df_topics.dropna(how='all', inplace=True)
        df_params.dropna(how='all', inplace=True)
    except Exception as e:
        st.error("❌ Failed to parse input tables from 'Query' sheet. Please check formatting.")
        st.stop()

    if not {'LAYER', 'METRICS', 'SEARCH PHRASE'}.issubset(df_params.columns) or \
       not {'TOPIC TYPE', 'ID TOPIC', 'TAG FOR VIRTUAL TOPIC'}.issubset(df_topics.columns):
        st.error("❗ Missing required columns in one of the tables.")
        st.stop()

    from_dt = (datetime.combine(date_range[0], datetime.min.time()) - timedelta(hours=7)).isoformat() + 'Z'
    to_dt = (datetime.combine(date_range[1], datetime.max.time()) - timedelta(hours=7)).isoformat() + 'Z'


    st.info("🔍 Processing topics...")

    headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9,vi;q=0.8",
            "authorization": AUTH_TOKEN,  # NOTE: No "Bearer " prefix needed if already full token
            "content-type": "application/json",
            "origin": "https://socialheat.younetmedia.com",
            "priority": "u=1, i",
            "referer": "https://socialheat.younetmedia.com/",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Linux"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }

    try:
        if export_buzz_button:
            export_total_buzz(sheet, df_topics, df_params, from_dt, to_dt, API_ENDPOINT, headers)

        if export_sources_button:
            export_top_sources(sheet, df_topics, df_params, from_dt, to_dt, API_ENDPOINT, headers)
        
        if export_posts_button:
            export_top_posts(sheet, df_topics, df_params, from_dt, to_dt, API_ENDPOINT, headers)

    except Exception as e:
        st.error(f"❌ Failed to export: {e}")
