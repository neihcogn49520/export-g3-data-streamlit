# export_top_sources.py

import requests
import pandas as pd
import json
from gspread_dataframe import set_with_dataframe
import streamlit as st

def export_top_sources(sheet, df_topics, df_params, from_dt, to_dt, api_endpoint, headers):
    st.info("🔍 Exporting Top 50 Sources...")
    all_export_data = []
    stt = 1

    for _, topic_row in df_topics.iterrows():
        topic_id_raw = str(topic_row.get('ID TOPIC', '')).strip()
        if not topic_id_raw.isdigit():
            continue

        topic_id = int(topic_id_raw)
        topic_type = topic_row.get('TOPIC TYPE', '').strip()
        topic_name = topic_row.get('TOPIC NAME', '').strip()
        virtual_tag = topic_row.get('TAG FOR VIRTUAL TOPIC', '')

        batch_requests = []

        for _, row in df_params.iterrows():
            search_phrase = row['SEARCH PHRASE']

            req = {
                "service": "topics/:topic_id/:service",
                "method": "find",
                "params": {
                    "route": {
                        "topic_id": topic_id,
                        "service": "top-sources"
                    },
                    "query": {
                        "$search": search_phrase,
                        "$date_from": from_dt,
                        "$date_to": to_dt,
                        "cache_version": 1750214164
                    }
                }
            }
            batch_requests.append(req)

        try:
            api_url = f"{api_endpoint}?topic_id={topic_id}&service=top-sources"
            resp = requests.post(api_url, headers=headers, data=json.dumps({"batch": batch_requests}))
            resp.raise_for_status()
            responses = resp.json()
        except Exception as e:
            st.error(f"❌ API call failed for topic {topic_id}: {e}")
            continue

        for i, r in enumerate(responses):
            param_row = df_params.iloc[i % len(df_params)]

            # Safely get the 'data' key if the response is a dict
            source_list = r.get("data", []) if isinstance(r, dict) else r

            if not isinstance(source_list, list):
                st.warning(f"⚠️ Unexpected data format in response {i}: {source_list}")
                continue

            for source_data in source_list[:50]:  # Top 50
                source_link = source_data.get("link") or source_data.get("source") or ""
                source_name = source_data.get("name", "")
                buzz_count = source_data.get("count", 0)

                # Build HYPERLINK for source name
                if source_link and source_name:
                    source_cell = f'=HYPERLINK("{source_link}", "{source_name}")'
                else:
                    source_cell = source_name or source_link

                all_export_data.append({
                    "STT": stt,
                    "ID TOPIC": topic_id,
                    "TOPIC NAME": topic_name,
                    "DATE RANGE": f"{from_dt[:10]} - {to_dt[:10]}",
                    "LAYER": param_row['LAYER'],
                    "METRICS": param_row['METRICS'],
                    "SEARCH PHRASE": search_phrase,
                    "SOURCE": source_cell,
                    "BUZZ": buzz_count
                })
                stt += 1


    if all_export_data:
        export_df = pd.DataFrame(all_export_data)
        try:
            try:
                sheet.del_worksheet(sheet.worksheet("Top 50 Sources"))
            except:
                pass
            ws = sheet.add_worksheet(title="Top 50 Sources", rows="2000", cols="20")
            set_with_dataframe(ws, export_df)
            st.success("✅ Export complete to sheet: Top 50 Sources")
        except Exception as e:
            st.error(f"❌ Failed to export: {e}")
    else:
        st.warning("⚠️ No data collected to export.")
