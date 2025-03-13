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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Use st.secrets to load Firebase configuration
firebase_cert_source = os.environ.get("FIREBASE_CERT_PATH") or st.secrets.get("FIREBASE_CERT_JSON")
firebase_db_url = os.environ.get("FIREBASE_DB_URL") or st.secrets.get("FIREBASE_DB_URL")

if not firebase_cert_source or not firebase_db_url:
    st.error("Firebase configuration is missing. Set FIREBASE (as dict) and FIREBASE_DB_URL in your secrets.")
    st.stop()

if isinstance(firebase_cert_source, dict):
    if "private_key" in firebase_cert_source:
        firebase_cert_source["private_key"] = firebase_cert_source["private_key"].replace("\\n", "\n")
    try:
        cred = credentials.Certificate(firebase_cert_source)
    except Exception as e:
        st.error("Failed to initialize certificate credential: " + str(e))
        st.stop()


try:
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': firebase_db_url})
        logging.info("Firebase Admin initialized successfully.")
    except ValueError as e:
        logging.info("Firebase Admin already initialized. Using existing app.")
        firebase_admin.get_app()
except Exception as e:
    logging.error("Error initializing Firebase Admin: %s", e)
    st.error("Firebase initialization failed. Check your configuration.")
    st.stop()


try:
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': firebase_db_url})
        logging.info("Firebase Admin initialized successfully.")
    except ValueError as e:
        logging.info("Firebase Admin already initialized. Using existing app.")
        firebase_admin.get_app()
except Exception as e:
    logging.error("Error initializing Firebase Admin: %s", e)
    st.error("Firebase initialization failed. Check your configuration.")
    st.stop()

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

st_autorefresh(interval=5000, limit=100, key="players_refresh")

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
        stats = compute_stats(players_df)
        if stats:
            st.subheader("Overall Player Statistics (PLAYERS)")
            st.write(f"Average Wins: {stats.get('average_win', 'N/A'):.2f}")
            st.write(f"Highest Wins: {stats.get('highest_win', 'N/A')}")
            st.write(f"UID for Highest Wins Player: {stats.get('uid_highest_win', 'N/A')}")
        else:
            st.write("Wins data not available to compute statistics.")
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
        st.subheader("Users with Common IP Addresses (PLAYERS)")
        common_ip_df = players_df.groupby("IP").filter(lambda group: len(group) > 1)
        if not common_ip_df.empty:
            total_common_users = common_ip_df.shape[0]
            distinct_common_ips = common_ip_df["IP"].nunique()
            st.write(f"Total number of players sharing a common IP: {total_common_users}")
            st.write(f"Number of distinct IP addresses shared: {distinct_common_ips}")
            st.dataframe(common_ip_df)
        else:
            st.write("No common IP addresses found within PLAYERS.")
    else:
        st.write("No player records found in the PLAYERS branch.")

st.header("TRACKING Data")
tracking_data_path = "TRACKING"
st.write(f"Fetching data from: {tracking_data_path}")

st_autorefresh(interval=5000, limit=100, key="tracking_refresh")

raw_tracking = fetch_data(tracking_data_path)
if raw_tracking is None:
    st.write("Waiting for TRACKING data... (Ensure your database is not empty)")
else:
    tracking_records = []
    for key, record in raw_tracking.items():
        if isinstance(record, dict):
            record["key"] = key
            tracking_records.append(record)
    if tracking_records:
        tracking_df = pd.DataFrame(tracking_records)
        st.subheader("TRACKING Data Table")
        st.dataframe(tracking_df)
        valid_tracking_ip_count = count_valid_tracking_ips(tracking_df)
        st.subheader("TRACKING IP Statistics")
        st.write(f"Number of users with a valid IP in TRACKING: {valid_tracking_ip_count}")
    else:
        st.write("No records found in the TRACKING branch.")

st.header("Matching Records between PLAYERS and TRACKING Based on Common IP")
if raw_players is None or raw_tracking is None:
    st.write("Waiting for both PLAYERS and TRACKING data...")
else:
    if 'players_df' in locals() and 'tracking_df' in locals() and not players_df.empty and not tracking_df.empty:
        merged_df = pd.merge(players_df, tracking_df, left_on="IP", right_on="ip", how="inner", suffixes=("_player", "_tracking"))
        if not merged_df.empty:
            st.subheader("Merged Records (Common IP)")
            st.write(f"Number of Matching Records: {len(merged_df)}")
            st.dataframe(merged_df)
        else:
            st.write("No matching IPs found between PLAYERS and TRACKING.")
    else:
        st.write("Insufficient data to perform matching.")
