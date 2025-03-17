import os
import json
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
from streamlit_autorefresh import st_autorefresh
import ipaddress

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Firebase configuration from environment variables or Streamlit secrets
firebase_cert_source = os.environ.get("FIREBASE_CERT_PATH") or st.secrets.get("FIREBASE_CERT_JSON")
firebase_db_url = os.environ.get("FIREBASE_DB_URL") or st.secrets.get("FIREBASE_DB_URL")

logging.info("Firebase DB URL: %s", firebase_db_url)
logging.info("Firebase Certificate Source Type: %s", type(firebase_cert_source))

if not firebase_cert_source or not firebase_db_url:
    st.error("Firebase configuration is missing. Set FIREBASE_CERT_JSON (as dict) and FIREBASE_DB_URL in your secrets.")
    st.stop()

# Convert to a regular dict if it's not one already (e.g., if it's an AttrDict)
if not isinstance(firebase_cert_source, dict):
    try:
        firebase_cert_source = dict(firebase_cert_source)
        logging.info("Converted firebase_cert_source to dict successfully.")
    except Exception as e:
        logging.error("Failed to convert certificate source to dict: %s", e)
        st.error("Failed to convert certificate source to dict: " + str(e))
        st.stop()

# Replace escaped newline characters with actual newlines in the private_key field
if "private_key" in firebase_cert_source:
    firebase_cert_source["private_key"] = firebase_cert_source["private_key"].replace("\\n", "\n")
    logging.info("Processed private_key newlines.")

# Initialize Firebase credentials
try:
    cred = credentials.Certificate(firebase_cert_source)
    logging.info("Certificate credential initialized successfully.")
except Exception as e:
    logging.error("Failed to initialize certificate credential: %s", e)
    st.error("Failed to initialize certificate credential: " + str(e))
    st.stop()

# Initialize Firebase Admin (only once)
try:
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': firebase_db_url})
        logging.info("Firebase Admin initialized successfully.")
    except ValueError:
        logging.info("Firebase Admin already initialized. Using existing app.")
        firebase_admin.get_app()
except Exception as e:
    logging.error("Error initializing Firebase Admin: %s", e)
    st.error("Firebase initialization failed. Check your configuration.")
    st.stop()

logging.info("Firebase Admin setup complete.")

def fetch_data(data_path):
    try:
        ref = db.reference(data_path)
        data = ref.get()
        logging.info("Fetched data from %s: %s", data_path, data)
        return data
    except Exception as e:
        logging.error("Error fetching data from %s: %s", data_path, e)
        return None

def compute_stats(df):
    stats = {}
    if "Wins" in df.columns:
        stats["average_win"] = df["Wins"].mean()
        stats["highest_win"] = df["Wins"].max()
        stats["uid_highest_win"] = df.loc[df["Wins"].idxmax()]["uid"]
    return stats

def compute_ip_stats(df):
    ipv4_count = 0
    ipv6_count = 0
    missing_count = 0
    if "IP" in df.columns:
        for ip in df["IP"]:
            if not isinstance(ip, str) or ip.strip() == "":
                missing_count += 1
            else:
                try:
                    ip_obj = ipaddress.ip_address(ip)
                    if ip_obj.version == 4:
                        ipv4_count += 1
                    elif ip_obj.version == 6:
                        ipv6_count += 1
                except Exception as e:
                    logging.warning("Invalid IP address '%s': %s", ip, e)
                    missing_count += 1
    else:
        missing_count = len(df)
    return {"ipv4_count": ipv4_count, "ipv6_count": ipv6_count, "missing_count": missing_count}

def filter_invalid_ips(df):
    invalid_records = []
    if "IP" in df.columns:
        for _, row in df.iterrows():
            ip = row["IP"]
            if not isinstance(ip, str) or ip.strip() == "":
                invalid_records.append(row)
            else:
                try:
                    ipaddress.ip_address(ip)
                except Exception as e:
                    invalid_records.append(row)
    return pd.DataFrame(invalid_records)

def count_valid_tracking_ips(df):
    valid_count = 0
    if "ip" in df.columns:
        for ip in df["ip"]:
            if isinstance(ip, str) and ip.strip() != "":
                try:
                    ipaddress.ip_address(ip)
                    valid_count += 1
                except Exception:
                    continue
    return valid_count

def merge_on_common_ip(players_df, tracking_df):
    if "IP" in players_df.columns and "ip" in tracking_df.columns:
        merged_df = pd.merge(players_df, tracking_df, left_on="IP", right_on="ip", how="inner", suffixes=("_player", "_tracking"))
        return merged_df
    else:
        return pd.DataFrame()

st.title("Realtime Firebase Data Dashboard (Admin Access)")
st.write("This dashboard polls data from the PLAYERS and TRACKING branches using the Admin SDK and displays various statistics, tables, and merged records.")

st.header("PLAYERS Data")
players_data_path = "PLAYERS"
st.write(f"Fetching data from: {players_data_path}")

st_autorefresh(interval=60000, limit=100, key="players_refresh")

raw_players = fetch_data(players_data_path)
if raw_players is None:
    st.write("Waiting for PLAYERS data... (Ensure your database is not empty)")
else:
    player_records = []
    for uid, record in raw_players.items():
        if isinstance(record, dict):
            record["uid"] = uid
            player_records.append(record)
    if player_records:
        players_df = pd.DataFrame(player_records)
        total_players = len(players_df)
        st.subheader("Total Number of Players (PLAYERS)")
        st.write(total_players)
        
        # Compute win-related statistics
        stats = compute_stats(players_df)
        if stats:
            st.subheader("Overall Player Statistics (PLAYERS)")
            st.write(f"Average Wins: {stats.get('average_win', 'N/A'):.2f}")
            st.write(f"Highest Wins: {stats.get('highest_win', 'N/A')}")
            st.write(f"UID for Highest Wins Player: {stats.get('uid_highest_win', 'N/A')}")
        else:
            st.write("Wins data not available to compute statistics.")
        
        # Calculate total ad revenue if available
        if "Ad_Revenue" in players_df.columns:
            # Ensure the values are numeric
            players_df["Ad_Revenue"] = pd.to_numeric(players_df["Ad_Revenue"], errors="coerce")
            total_ad_revenue = players_df["Ad_Revenue"].sum()
            st.subheader("Total Ad Revenue (PLAYERS)")
            st.write(f"${total_ad_revenue/100:,.2f}")
        else:
            st.write("Ad Revenue data not available in PLAYERS.")
        
        organic_df = players_df[players_df["Source"].str.lower() == "organic"]
        pubscale_df = players_df[players_df["Source"].str.lower() == "pubscale"]
        st.subheader("Source Statistics (PLAYERS)")
        st.write(f"Number of Organic Players: {organic_df.shape[0]}")
        st.write(f"Number of Pubscale Players: {pubscale_df.shape[0]}")
        
        st.subheader("All Organic Players (PLAYERS)")
        if not organic_df.empty:
            st.dataframe(organic_df)
        else:
            st.write("No players with Source 'organic' found in PLAYERS.")
        
        st.subheader("All Pubscale Players (PLAYERS)")
        if not pubscale_df.empty:
            st.dataframe(pubscale_df)
        else:
            st.write("No players with Source 'pubscale' found in PLAYERS.")
            
        ip_stats = compute_ip_stats(players_df)
        st.subheader("IP Address Statistics (PLAYERS)")
        st.write(f"Number of Players with IPv4: {ip_stats.get('ipv4_count', 0)}")
        st.write(f"Number of Players with IPv6: {ip_stats.get('ipv6_count', 0)}")
        st.write(f"Number of Players with Missing/Invalid IP: {ip_stats.get('missing_count', 0)}")
        
        invalid_ip_df = filter_invalid_ips(players_df)
        if not invalid_ip_df.empty:
            st.subheader("Players with Missing/Invalid IP Addresses (PLAYERS)")
            st.dataframe(invalid_ip_df)
        else:
            st.write("No players with missing or invalid IP addresses in PLAYERS.")