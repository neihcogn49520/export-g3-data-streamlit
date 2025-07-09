# For Streamlit
import streamlit as st

def get_total_buzz(data_rows):
    if not isinstance(data_rows, list):
        return 0

    total = 0
    for day in data_rows:
        if isinstance(day, dict) and "count" in day:
            total += day["count"]
        else:
            st.warning(f"Skipped row: {day}")
    return total

data_rows = [
    {
        "time": "2025-06-30T17:00:00Z",
        "count": 2395,
        "mention_type": [
            {"val": "COMMENT", "count": 2295},
            {"val": "POST", "count": 94},
            {"val": "SHARE", "count": 6}
        ]
    },
    {
        "time": "2025-07-01T17:00:00Z",
        "count": 2833,
        "mention_type": [
            {"val": "COMMENT", "count": 2721},
            {"val": "POST", "count": 102},
            {"val": "SHARE", "count": 10}
        ]
    },
    {
        "time": "2025-07-02T17:00:00Z",
        "count": 3317,
        "mention_type": [
            {"val": "COMMENT", "count": 3211},
            {"val": "POST", "count": 86},
            {"val": "SHARE", "count": 20}
        ]
    },
    {
        "time": "2025-07-03T17:00:00Z",
        "count": 2951,
        "mention_type": [
            {"val": "COMMENT", "count": 2865},
            {"val": "POST", "count": 84},
            {"val": "SHARE", "count": 2}
        ]
    },
    {
        "time": "2025-07-04T17:00:00Z",
        "count": 2192,
        "mention_type": [
            {"val": "COMMENT", "count": 2112},
            {"val": "POST", "count": 80}
        ]
    },
    {
        "time": "2025-07-05T17:00:00Z",
        "count": 1862,
        "mention_type": [
            {"val": "COMMENT", "count": 1810},
            {"val": "POST", "count": 36},
            {"val": "SHARE", "count": 16}
        ]
    }
] # your full JSON pasted here
print(get_total_buzz(data_rows))  # Should return 15550
