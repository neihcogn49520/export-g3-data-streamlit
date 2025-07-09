# export_total_buzz.py

import requests
import pandas as pd
import json
from gspread_dataframe import set_with_dataframe
import streamlit as st

def get_total_buzz(data_rows):
    if not isinstance(data_rows, list):
        return 0
    return sum(day.get("count", 0) for day in data_rows if isinstance(day, dict))

def export_total_buzz(sheet, df_topics, df_params, from_dt, to_dt, api_endpoint, headers):
    st.info("🔍 Exporting Total Buzz...")
    all_export_data = []
    stt = 1

    for _, topic_row in df_topics.iterrows():
        topic_id_raw = str(topic_row.get('ID TOPIC', '')).strip()
        if not topic_id_raw.isdigit():
            continue

        topic_id = int(topic_id_raw)
        topic_type = topic_row.get('TOPIC TYPE', '').strip()
        topic_name = topic_row.get('TOPIC NAME', '').strip()

        batch_requests = []

        for _, row in df_params.iterrows():
            search_phrase = row['SEARCH PHRASE']

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

        try:
            api_url = f"{api_endpoint}?topic_id={topic_id}&service=mentions-trendline"
            resp = requests.post(api_url, headers=headers, data=json.dumps({"batch": batch_requests}))
            resp.raise_for_status()
            responses = resp.json()
        except Exception as e:
            st.error(f"❌ API call failed for topic {topic_id}: {e}")
            continue

        for i, r in enumerate(responses):
            param_row = df_params.iloc[i % len(df_params)]
            
            data_rows = r
            total_buzz = get_total_buzz(data_rows)
            all_export_data.append({
                "STT": stt,
                "ID TOPIC": topic_id,
                "TOPIC NAME": topic_name,
                "DATE RANGE": f"{from_dt[:10]} - {to_dt[:10]}",
                "LAYER": param_row['LAYER'],
                "METRICS": param_row['METRICS'],
                "SEARCH PHRASE": search_phrase,
                "TOTAL BUZZ": total_buzz
            })
            stt += 1

    if all_export_data:
        export_df = pd.DataFrame(all_export_data)
        try:
            try:
                sheet.del_worksheet(sheet.worksheet("Total Buzz"))
            except:
                pass
            ws = sheet.add_worksheet(title="Total Buzz", rows="1000", cols="20")
            set_with_dataframe(ws, export_df)
            st.success("✅ Export complete to sheet: Total Buzz")
        except Exception as e:
            st.error(f"❌ Failed to export: {e}")
    else:
        st.warning("⚠️ No data collected to export.")
