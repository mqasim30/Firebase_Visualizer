import os
import json
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Firebase configuration from environment variables or Streamlit secrets
firebase_cert_source = os.environ.get("FIREBASE_CERT_PATH") or st.secrets.get("FIREBASE_CERT_JSON")
firebase_db_url = os.environ.get("FIREBASE_DB_URL") or st.secrets.get("FIREBASE_DB_URL")

logging.info("Firebase DB URL: %s", firebase_db_url)

if not firebase_cert_source or not firebase_db_url:
    st.error("Firebase configuration is missing. Set FIREBASE_CERT_JSON (as dict) and FIREBASE_DB_URL in your secrets.")
    st.stop()

# Convert to a regular dict if it's not one already
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

# Function to fetch the latest 10 conversions efficiently
def fetch_latest_conversions(limit=10):
    try:
        all_conversions = []
        
        # Get all users with conversions
        user_ref = database.reference("CONVERSIONS")
        user_data = user_ref.get(shallow=True)
        
        if not user_data or not isinstance(user_data, dict):
            return []
            
        # For each user, get their conversions
        for user_id in user_data.keys():
            user_conv_ref = database.reference(f"CONVERSIONS/{user_id}")
            # Get conversion IDs
            user_conv_ids = user_conv_ref.get(shallow=True)
            
            if not user_conv_ids or not isinstance(user_conv_ids, dict):
                continue
                
            # For each conversion type, get the data
            for conv_id in user_conv_ids.keys():
                # Get this specific conversion
                conv_ref = database.reference(f"CONVERSIONS/{user_id}/{conv_id}")
                conv_data = conv_ref.get()
                
                if not isinstance(conv_data, dict):
                    continue
                    
                # Add this conversion to our list
                conversion = {
                    "user_id": user_id,
                    "conversion_id": conv_id,
                    **conv_data
                }
                all_conversions.append(conversion)
        
        # Sort by time (descending) and take the latest ones
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

# --- LATEST PLAYERS SECTION ---
st.header("Latest 10 Players")

with st.spinner("Loading latest players..."):
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

with st.spinner("Loading latest conversions..."):
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
