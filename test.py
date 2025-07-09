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

# === UI ===
st.title("📊 Batch API Export Tool")
sheet_url = st.text_input("🔗 Enter Google Sheet URL")
default_range = (date.today() - timedelta(days=7), date.today())
date_range = st.date_input("Select Date Range", default_range, format="YYYY-MM-DD")

export_buzz_button = st.button("🚀 Export Total Buzz")
export_sources_button = st.button("📊 Export Top 50 Sources")
export_posts_button = st.button("🔥 Export Top 10 Posts")

# === Validate Google Sheet URL ===
if not sheet_url:
    st.error("❗ Please enter a Google Sheet URL.")
    st.stop()
def extract_sheet_id(url):
    import re
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
    return match.group(1) if match else None
sheet_id = extract_sheet_id(sheet_url)
if not sheet_id:
    st.error("❌ Invalid Google Sheet URL. Please check and try again.")
    st.stop()



if export_button:
    try:
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

    from_dt = datetime.combine(date_range[0], datetime.min.time()).isoformat() + 'Z'
    to_dt = datetime.combine(date_range[1], datetime.max.time()).isoformat() + 'Z'

    st.info("🔍 Processing topics...")

    # === Helper function
    def get_total_buzz(data_rows):
        if not isinstance(data_rows, list):
            return 0
        return sum(day.get("count", 0) for day in data_rows if isinstance(day, dict))

    # ✅ GLOBAL data
    all_export_data = []
    stt = 1

    for _, topic_row in df_topics.iterrows():
        topic_id_raw = str(topic_row.get('ID TOPIC', '')).strip()
        if not topic_id_raw.isdigit():
            print(f"⚠️ Skipping invalid topic ID: {topic_id_raw}")
            continue

        topic_id = int(topic_id_raw)
        topic_type = topic_row.get('TOPIC TYPE', '').strip()
        topic_name = topic_row.get('TOPIC NAME', '').strip()
        virtual_tag = topic_row.get('TAG FOR VIRTUAL TOPIC', '')

        topic_params = df_params.copy()
        if topic_params.empty:
            continue

        batch_requests = []
        search_phrases = []

        for _, row in topic_params.iterrows():
            search_phrase = row['SEARCH PHRASE']
            if topic_type.lower() == "virtual" and virtual_tag:
                search_phrase += f" AND {virtual_tag}"
            search_phrases.append(search_phrase)

            req = {
                "service": "topics/:topic_id/:service",
                "method": "find",
                "params": {
                    "route": {
                        "topic_id": topic_id,
                        "service": "mentions-trendline"
                    },
                    "query": {
                        "$range_type": "daily",
                        "$group_by": "mention_type",
                        "$noise_filter_mode": "EXCLUDE_NOISE_SPAM",
                        "$source_group_not_in": "off",
                        "$dashboard": 26036,
                        "$search": search_phrase,
                        "$date_from": from_dt,
                        "$date_to": to_dt,
                        "cache_version": 1750214164
                    }
                }
            }
            batch_requests.append(req)

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
            api_url = f"{API_ENDPOINT}?topic_id={topic_id}&service=mentions-trendline"
            resp = requests.post(api_url, headers=headers, data=json.dumps({"batch": batch_requests}))
            resp.raise_for_status()
            responses = resp.json()
        except Exception as e:
            st.error(f"❌ API call failed for topic {topic_id}: {e}")
            continue

        # ✅ Collect results for this topic
        for i, r in enumerate(responses):
            param_row = topic_params.iloc[i % len(topic_params)]
            search_phrase_used = search_phrases[i % len(search_phrases)]
            data_rows = r
            total_buzz = get_total_buzz(data_rows)
            st.json(data_rows)
            all_export_data.append({
                "STT": stt,
                "ID TOPIC": topic_id,
                "TOPIC NAME": topic_name,
                "DATE RANGE": f"{from_dt[:10]} - {to_dt[:10]}",
                "LAYER": param_row['LAYER'],
                "METRICS": param_row['METRICS'],
                "COMBINED PHRASE": search_phrase_used,
                "TOTAL BUZZ": total_buzz
            })
            stt += 1

    # ✅ Export once at the end
    if all_export_data:
        export_df = pd.DataFrame(all_export_data)
        export_sheet_name = "Total Buzz"
        try:
            try:
                sheet.del_worksheet(sheet.worksheet(export_sheet_name))
            except:
                pass
            ws = sheet.add_worksheet(title=export_sheet_name, rows="1000", cols="20")
            set_with_dataframe(ws, export_df)
            st.success("✅ Export complete to sheet: Total Buzz")
        except Exception as e:
            st.error(f"❌ Failed to export: {e}")
    else:
        st.warning("⚠️ No data collected to export.")

