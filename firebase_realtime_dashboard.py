import os
import json
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
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

# Fetch data with NO caching to ensure fresh data on every page load
def fetch_data(data_path):
    try:
        ref = database.reference(data_path)
        data = ref.get()
        logging.info("Fetched data from %s", data_path)
        return data
    except Exception as e:
        logging.error("Error fetching data from %s: %s", data_path, e)
        return None

# Get database reference
def get_database():
    return db

database = get_database()

# Function to fetch the latest 10 players using the index on Install_time
def fetch_latest_players(limit=10):
    try:
        ref = database.reference("PLAYERS")
        # Order by Install_time descending and limit to last 10 entries
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

# New function to fetch the latest 10 conversions with properly handling nested structures
def fetch_latest_conversions(limit=10):
    try:
        # First, get all conversions data
        ref = database.reference("CONVERSIONS")
        all_data = ref.get()
        
        if not all_data or not isinstance(all_data, dict):
            logging.error("No valid CONVERSIONS data found")
            return []
        
        # Flatten the nested structure
        all_conversions = []
        
        for user_id, user_data in all_data.items():
            if not isinstance(user_data, dict):
                continue
                
            for win_id, conversion_data in user_data.items():
                if not isinstance(conversion_data, dict):
                    continue
                    
                # Extract the conversion data with proper IDs
                conversion = {
                    "user_id": user_id,
                    "conversion_id": win_id,
                    **conversion_data  # This adds goal, source, time
                }
                all_conversions.append(conversion)
        
        # Sort by time (descending) and take the latest 10
        sorted_conversions = sorted(
            all_conversions, 
            key=lambda x: x.get("time", 0), 
            reverse=True
        )
        
        return sorted_conversions[:limit]
        
    except Exception as e:
        logging.error(f"Error fetching latest conversions: {e}")
        return []

# Format timestamp to human-readable date
def format_timestamp(timestamp):
    if pd.notna(timestamp) and timestamp != 0:
        try:
            return datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return "Invalid date"
    return "Not available"

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

# --- TITLE ---
st.title("Firebase Game Analytics Dashboard")

# Fetch player data
players_data_path = "PLAYERS"
raw_players = fetch_data(players_data_path)

if raw_players is None:
    st.error("Waiting for PLAYERS data... (Ensure your database is not empty)")
else:
    # Build player records
    player_records = [{"uid": uid, **record} for uid, record in raw_players.items() if isinstance(record, dict)]
    
    if player_records:
        players_df = pd.DataFrame(player_records)
        
        # KEY METRICS
        col1, col2, col3 = st.columns(3)
        
        # Total Players
        total_players = len(players_df)
        with col1:
            st.subheader("Total Players")
            st.markdown(f"<p class='big-value'>{total_players}</p>", unsafe_allow_html=True)
        
        # Ad Revenue
        if "Ad_Revenue" in players_df.columns:
            players_df["Ad_Revenue"] = pd.to_numeric(players_df["Ad_Revenue"], errors="coerce")
            total_ad_revenue = players_df["Ad_Revenue"].sum()
            with col2:
                st.subheader("Total Ad Revenue")
                st.markdown(f"<p class='big-value'>${total_ad_revenue/100:,.2f}</p>", unsafe_allow_html=True)
        
        # Impressions
        if "Impressions" in players_df.columns:
            players_df["Impressions"] = pd.to_numeric(players_df["Impressions"], errors="coerce")
            total_impressions = players_df["Impressions"].sum()
            with col3:
                st.subheader("Total Impressions")
                st.markdown(f"<p class='big-value'>{total_impressions:,.0f}</p>", unsafe_allow_html=True)
        
        # SOURCE BREAKDOWN
        st.subheader("Source Statistics")
        
        # Filter players by source (case-insensitive)
        cols = st.columns(3)
        
        organic_df = players_df[players_df["Source"].str.lower() == "organic"]
        pubscale_df = players_df[players_df["Source"].str.lower() == "pubscale"]
        timebucks_df = players_df[players_df["Source"].str.lower() == "timebucks"]
        
        with cols[0]:
            st.metric("Organic Players", organic_df.shape[0])
        
        with cols[1]:
            st.metric("Pubscale Players", pubscale_df.shape[0])
        
        with cols[2]:
            st.metric("Timebucks Players", timebucks_df.shape[0])

# --- LATEST PLAYERS SECTION ---
st.header("Latest 10 Players")

# Fetch the latest 10 players
latest_players = fetch_latest_players(10)

if not latest_players:
    st.warning("No recent players found or Install_time field not available")
else:
    # Create DataFrame from the latest players data
    latest_df = pd.DataFrame(latest_players)
    
    # Format the Install_time to be more readable
    if "Install_time" in latest_df.columns:
        latest_df["Formatted_Install_time"] = latest_df["Install_time"].apply(format_timestamp)
        # Sort the data by Install_time
        latest_df = latest_df.sort_values(by="Install_time", ascending=False)
    
    # Display key information in a clean table
    display_cols = ["uid", "Formatted_Install_time", "Source", "Geo", "IP", "Wins", "Goal", "Impressions", "Ad_Revenue"]
    display_cols = [col for col in display_cols if col in latest_df.columns]
    
    st.dataframe(latest_df[display_cols])

# --- LATEST CONVERSIONS SECTION ---
st.header("Latest 10 Conversions")

# Fetch the latest 10 conversions with the fixed function
latest_conversions = fetch_latest_conversions(10)

if not latest_conversions:
    st.warning("No conversions found. Make sure your CONVERSIONS data is properly structured.")
else:
    # Create DataFrame from the latest conversions data
    conversions_df = pd.DataFrame(latest_conversions)
    
    # Format the time to be more readable
    if "time" in conversions_df.columns:
        conversions_df["Formatted_time"] = conversions_df["time"].apply(format_timestamp)
    
    # Display the conversion information with all relevant fields
    display_cols = ["user_id", "conversion_id", "Formatted_time", "goal", "source"]
    display_cols = [col for col in display_cols if col in conversions_df.columns]
    
    st.dataframe(conversions_df[display_cols])
