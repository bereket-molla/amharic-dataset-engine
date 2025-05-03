import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(str(root))

import streamlit as st
import sqlite3
import pandas as pd
from configs.constants import DB_PATH

st.set_page_config(page_title="Amharic Message Dataset", layout="wide")
st.title("Amharic Dataset Dashboard (Cleaned Messages Only)")

conn = sqlite3.connect(DB_PATH)
msg_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
channel_count = conn.execute("SELECT COUNT(DISTINCT channel) FROM messages").fetchone()[0]

col1, col2 = st.columns(2)
col1.metric("Total Messages", f"{msg_count:,}")
col2.metric("Channels Covered", f"{channel_count}")

st.markdown("---")

st.subheader("Latest Collected Messages")

df = pd.read_sql_query("""
    SELECT msg_id, channel, text, date, url
    FROM messages
    ORDER BY date DESC
    LIMIT 100
""", conn)

df["link"] = df["url"].apply(lambda u: f"[Link]({u})")
st.dataframe(df[["date", "channel", "text", "link"]], use_container_width=True)

conn.close()
