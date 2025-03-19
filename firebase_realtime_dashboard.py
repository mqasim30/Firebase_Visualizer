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
from datetime import datetime

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

# Optionally, cache Firebase Admin initialization (if needed) using st.cache_resource
@st.cache_resource(show_spinner=False)
def get_database():
    return db

database = get_database()

# Cache data fetching for 60 seconds to reduce redundant downloads.
@st.cache_data(ttl=60, show_spinner=False)
def fetch_data(data_path):
    try:
        ref = database.reference(data_path)
        data = ref.get()
        logging.info("Fetched data from %s: %s", data_path, data)
        return data
    except Exception as e:
        logging.error("Error fetching data from %s: %s", data_path, e)
        return None

# New function to fetch the latest 10 players using the index on Install_time
@st.cache_data(ttl=60, show_spinner=False)
def fetch_latest_players(limit=10):
    try:
        ref = database.reference("PLAYERS")
        # Order by Install_time descending and limit to last 10 entries
        # This will utilize the .indexOn rule we set up
        query = ref.order_by_child("Install_time").limit_to_last(limit)
        data = query.get()
        logging.info(f"Fetched latest {limit} players based on Install_time")
        if data:
            # Convert to list of records with UID included
            latest_players = [{"uid": uid, **record} for uid, record in data.items() if isinstance(record, dict)]
            return latest_players
        return []
    except Exception as e:
        logging.error(f"Error fetching latest players: {e}")
        return []

def compute_stats(df):
    stats = {}
    if "Wins" in df.columns:
        stats["average_win"] = df["Wins"].mean()
        stats["highest_win"] = df["Wins"].max()
        stats["uid_highest_win"] = df.loc[df["Wins"].idxmax()]["uid"]
    return stats

def compute_ip_stats(df):
    if "IP" in df.columns:
        def ip_version(ip):
            try:
                return ipaddress.ip_address(ip).version if isinstance(ip, str) and ip.strip() != "" else None
            except Exception:
                return None
        versions = df["IP"].apply(ip_version)
        ipv4_count = (versions == 4).sum()
        ipv6_count = (versions == 6).sum()
        missing_count = versions.isna().sum()
    else:
        ipv4_count = 0
        ipv6_count = 0
        missing_count = len(df)
    return {"ipv4_count": ipv4_count, "ipv6_count": ipv6_count, "missing_count": missing_count}

def filter_invalid_ips(df):
    def is_valid_ip(ip):
        try:
            ipaddress.ip_address(ip)
            return True
        except Exception:
            return False
    valid_mask = df["IP"].apply(lambda ip: isinstance(ip, str) and ip.strip() != "" and is_valid_ip(ip))
    return df[~valid_mask]

def count_valid_tracking_ips(df):
    if "ip" in df.columns:
        def is_valid(ip):
            try:
                ipaddress.ip_address(ip)
                return True
            except Exception:
                return False
        valid_mask = df["ip"].apply(lambda ip: isinstance(ip, str) and ip.strip() != "" and is_valid(ip))
        return valid_mask.sum()
    return 0

def merge_on_common_ip(players_df, tracking_df):
    if "IP" in players_df.columns and "ip" in tracking_df.columns:
        merged_df = pd.merge(players_df, tracking_df, left_on="IP", right_on="ip", how="inner", suffixes=("_player", "_tracking"))
        return merged_df
    else:
        return pd.DataFrame()

# Format timestamp to human-readable date
def format_timestamp(timestamp):
    if pd.notna(timestamp) and timestamp != 0:
        try:
            return datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return "Invalid date"
    return "Not available"

# Set up auto-refresh every 1 minute
st_autorefresh(interval=60000, limit=100, key="players_refresh")

players_data_path = "PLAYERS"
raw_players = fetch_data(players_data_path)

# Add custom CSS for larger text values
st.markdown(
    """
    <style>
    .big-value {
        font-size: 24px !important;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)

if raw_players is None:
    st.write("Waiting for PLAYERS data... (Ensure your database is not empty)")
else:
    # Build player records using dictionary comprehension.
    player_records = [{"uid": uid, **record} for uid, record in raw_players.items() if isinstance(record, dict)]
    if player_records:
        players_df = pd.DataFrame(player_records)
        total_players = len(players_df)
        st.subheader("Total Number of Players (PLAYERS)")
        st.markdown(f"<p class='big-value'>{total_players}</p>", unsafe_allow_html=True)
        
        if "Ad_Revenue" in players_df.columns:
            players_df["Ad_Revenue"] = pd.to_numeric(players_df["Ad_Revenue"], errors="coerce")
            total_ad_revenue = players_df["Ad_Revenue"].sum()
            st.subheader("Total Ad Revenue (PLAYERS)")
            st.markdown(f"<p class='big-value'>${total_ad_revenue/100:,.2f}</p>", unsafe_allow_html=True)
        else:
            st.write("Ad Revenue data not available in PLAYERS.")

        if "Impressions" in players_df.columns:
            players_df["Impressions"] = pd.to_numeric(players_df["Impressions"], errors="coerce")
            total_impressions = players_df["Impressions"].sum()
            st.subheader("Total Impressions (PLAYERS)")
            st.markdown(f"<p class='big-value'>{total_impressions}</p>", unsafe_allow_html=True)
        else:
            st.write("Impressions data not available in PLAYERS.")
        
        # Filter players by source (case-insensitive)
        organic_df = players_df[players_df["Source"].str.lower() == "organic"]
        pubscale_df = players_df[players_df["Source"].str.lower() == "pubscale"]
        timebucks_df = players_df[players_df["Source"].str.lower() == "timebucks"]

        st.subheader("Source Statistics (PLAYERS)")
        st.markdown(f"<p class='big-value'>Number of Organic Players: {organic_df.shape[0]}</p>", unsafe_allow_html=True)
        st.markdown(f"<p class='big-value'>Number of Pubscale Players: {pubscale_df.shape[0]}</p>", unsafe_allow_html=True)
        st.markdown(f"<p class='big-value'>Number of Timebucks Players: {timebucks_df.shape[0]}</p>", unsafe_allow_html=True)
        
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
        
        st.subheader("All Timebucks Players (PLAYERS)")
        if not timebucks_df.empty:
            st.dataframe(timebucks_df)
        else:
            st.write("No players with Source 'timebucks' found in PLAYERS.")
            
# --- Tracking Table Section ---
tracking_data_path = "TRACKING"
raw_tracking = fetch_data(tracking_data_path)

if raw_tracking is None:
    st.write("Waiting for TRACKING data... (Ensure your database is not empty)")
else:
    # Build tracking records using dictionary comprehension.
    tracking_records = [{"key": key, **record} for key, record in raw_tracking.items() if isinstance(record, dict)]
    if tracking_records:
        tracking_df = pd.DataFrame(tracking_records)
        st.subheader("Tracking Data (TRACKING)")
        st.dataframe(tracking_df)
        
        # New Section: Non-IN Geo in TRACKING
        if "geo" in tracking_df.columns:
            # Normalize geo values: fill NaN with empty string, strip, and convert to uppercase.
            tracking_df["geo"] = tracking_df["geo"].fillna("").astype(str).str.strip().str.upper()
            non_in_tracking_df = tracking_df[(tracking_df["geo"] != "IN") & (tracking_df["geo"] != "")]
            non_in_tracking_count = non_in_tracking_df.shape[0]
            st.subheader("Non-IN Geo Count in Tracking (TRACKING)")
            st.markdown(f"<p class='big-value'>{non_in_tracking_count}</p>", unsafe_allow_html=True)
            if not non_in_tracking_df.empty:
                st.subheader("Entries with Non-IN Geo in Tracking (TRACKING)")
                st.dataframe(non_in_tracking_df)
            else:
                st.write("No tracking records with Geo different from 'IN'.")
        else:
            st.write("Geo field not available in TRACKING.")
    else:
        st.write("No tracking records found in the TRACKING branch.")

# --- Latest Players Section (New) ---
st.header("Latest 10 Players")
st.write("This section shows the 10 most recently installed players based on Install_time")

# Fetch the latest 10 players using our new function
latest_players = fetch_latest_players(10)

if not latest_players:
    st.write("No recent players found or Install_time field not available")
else:
    # Create DataFrame from the latest players data
    latest_df = pd.DataFrame(latest_players)
    
    # Create a copy of the dataframe for display
    display_df = latest_df.copy()
    
    # Format the Install_time to be more readable but keep original for sorting
    if "Install_time" in display_df.columns:
        display_df["Formatted_Install_time"] = display_df["Install_time"].apply(format_timestamp)
        # Sort the data by Install_time before selecting display columns
        display_df = display_df.sort_values(by="Install_time", ascending=False)
    
    # Display key information in a clean table
    display_cols = ["uid", "Formatted_Install_time", "Source", "Geo", "IP", "Wins", "Goal", "Impressions", "Ad_Revenue"]
    display_cols = [col for col in display_cols if col in display_df.columns]
    
    st.subheader("Latest Players Information")
    st.dataframe(display_df[display_cols])