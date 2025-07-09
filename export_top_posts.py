import requests
import pandas as pd
import json
from gspread_dataframe import set_with_dataframe
import streamlit as st

def export_top_posts(sheet, df_topics, df_params, from_dt, to_dt, api_endpoint, headers):
    st.info("🔍 Exporting Top Posts...")
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
                        "service": "top-posts-by-field"
                    },
                    "query": {
                        "$search": search_phrase,
                        "$date_from": from_dt,
                        "$date_to": to_dt,
                        "$skip": 0,
                        "$limit": 10,
                        "$visualization_series": "total_mentions",
                        "$visualize_sentiment_comments": False,
                        "$return_zero_value": 1,
                        "$noise_filter_mode": "EXCLUDE_NOISE_SPAM"
                    }
                }
            }
            batch_requests.append(req)

        try:
            api_url = f"{api_endpoint}?topic_id={topic_id}&service=top-posts-by-field"
            resp = requests.post(api_url, headers=headers, data=json.dumps({"batch": batch_requests}))
            resp.raise_for_status()
            responses = resp.json()
        except Exception as e:
            st.error(f"❌ API call failed for topic {topic_id}: {e}")
            continue

        for i, r in enumerate(responses):
            param_row = df_params.iloc[i % len(df_params)]
            posts = r.get("data", []) if isinstance(r, dict) else []

            for post_data in posts[:10]:  # Top 10 posts
                post_link = post_data.get("link", "").strip()
                short_content = post_data.get("short_content", "").strip().replace('"', '')  # Remove double quotes
                source_link = post_data.get("source_link", "")
                source_name = post_data.get("source_name", "")
                buzz_count = post_data.get("total_mentions", 0)

                # Truncate content to 100 characters
                display_text = short_content[:100] + "..." if len(short_content) > 100 else short_content

                # Build HYPERLINK formula for Google Sheets
                if post_link:
                    post_cell = f'=HYPERLINK("{post_link}", "{display_text}")'
                else:
                    post_cell = display_text  # fallback: plain text if no link
                
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
                    "COMBINED PHRASE": search_phrase,
                    "POST": post_cell,
                    "SOURCE": source_cell,
                    "BUZZ": buzz_count
                })
                stt += 1

    if all_export_data:
        export_df = pd.DataFrame(all_export_data)
        try:
            try:
                sheet.del_worksheet(sheet.worksheet("Top 10 Posts"))
            except:
                pass
            ws = sheet.add_worksheet(title="Top 10 Posts", rows="2000", cols="20")
            set_with_dataframe(ws, export_df)
            st.success("✅ Export complete to sheet: Top Posts")
        except Exception as e:
            st.error(f"❌ Failed to export: {e}")
    else:
        st.warning("⚠️ No data collected to export.")