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
from datetime import datetime, timedelta

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

# Get database reference
def get_database():
    return db

database = get_database()

# Function to get player count using shallow query (more efficient than fetching all data)
def get_player_count():
    try:
        ref = database.reference("PLAYERS")
        # Use shallow=True to get only keys at this level
        count_data = ref.get(shallow=True)
        if count_data and isinstance(count_data, dict):
            return len(count_data)
        return 0
    except Exception as e:
        logging.error(f"Error getting player count: {e}")
        return 0

# Function to fetch players by source (more efficient than fetching all)
def get_players_by_source(source_name):
    try:
        ref = database.reference("PLAYERS")
        # Query players where Source equals the source name (case-insensitive not supported in Firebase)
        # We'll use startAt and endAt to handle mixed case
        source_lower = source_name.lower()
        source_upper = source_name.upper()
        
        # Try with exact match first
        query = ref.order_by_child("Source").equal_to(source_name)
        exact_match = query.get()
        
        # Try lowercase
        query_lower = ref.order_by_child("Source").equal_to(source_lower)
        lower_match = query_lower.get()
        
        # Try uppercase
        query_upper = ref.order_by_child("Source").equal_to(source_upper)
        upper_match = query_upper.get()
        
        # Combine results
        result = {}
        if exact_match and isinstance(exact_match, dict):
            result.update(exact_match)
        if lower_match and isinstance(lower_match, dict):
            result.update(lower_match)
        if upper_match and isinstance(upper_match, dict):
            result.update(upper_match)
            
        return len(result)
    except Exception as e:
        logging.error(f"Error getting players by source {source_name}: {e}")
        return 0

# Function to get total ad revenue and impressions (this is more complex to optimize)
def get_ad_metrics():
    try:
        # Unfortunately, Firebase doesn't support SUM operations, so we have to use some sampling
        # Set a reasonable limit to prevent downloading the entire database
        ref = database.reference("PLAYERS")
        
        # Get the latest 100 records to estimate average values
        sample = ref.order_by_child("Install_time").limit_to_last(100).get()
        
        if not sample or not isinstance(sample, dict):
            return 0, 0
            
        # Calculate total and average per player
        total_ad_revenue = 0
        total_impressions = 0
        
        for player_data in sample.values():
            if not isinstance(player_data, dict):
                continue
                
            # Add ad revenue
            try:
                ad_revenue = float(player_data.get("Ad_Revenue", 0))
                total_ad_revenue += ad_revenue
            except (ValueError, TypeError):
                pass
                
            # Add impressions
            try:
                impressions = int(player_data.get("Impressions", 0))
                total_impressions += impressions
            except (ValueError, TypeError):
                pass
        
        # Get total player count
        player_count = get_player_count()
        
        # If we have no players, return 0
        if player_count == 0 or len(sample) == 0:
            return 0, 0
            
        # Calculate average per player
        avg_revenue_per_player = total_ad_revenue / len(sample)
        avg_impressions_per_player = total_impressions / len(sample)
        
        # Estimate total based on average * total count
        estimated_total_revenue = avg_revenue_per_player * player_count
        estimated_total_impressions = avg_impressions_per_player * player_count
        
        return estimated_total_revenue, estimated_total_impressions
        
    except Exception as e:
        logging.error(f"Error getting ad metrics: {e}")
        return 0, 0

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

# Function to fetch the latest conversions efficiently using indexing
def fetch_latest_conversions_efficiently(limit=10):
    try:
        all_conversions = []
        
        # This approach works if we index the time field at the user level
        # Get all users with conversions
        user_ref = database.reference("CONVERSIONS")
        user_data = user_ref.get(shallow=True)
        
        if not user_data or not isinstance(user_data, dict):
            return []
            
        # For each user, get their latest conversion
        for user_id in user_data.keys():
            user_conv_ref = database.reference(f"CONVERSIONS/{user_id}")
            # Use shallow to just get the conversion IDs first
            user_conv_ids = user_conv_ref.get(shallow=True)
            
            if not user_conv_ids or not isinstance(user_conv_ids, dict):
                continue
                
            # For each conversion type, get the latest entries
            for conv_id in user_conv_ids.keys():
                # Get this specific conversion
                conv_ref = database.reference(f"CONVERSIONS/{user_id}/{conv_id}")
                # We can use this direct path because we know the structure
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
        logging.error(f"Error fetching latest conversions efficiently: {e}")
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

# Progress bar while loading
with st.spinner("Loading dashboard data..."):
    # KEY METRICS - Using optimized queries
    
    # Get player count (efficient shallow query)
    total_players = get_player_count()
    
    # Get ad metrics (using sampling for efficiency)
    total_ad_revenue, total_impressions = get_ad_metrics()
    
    # Get source counts (using indexed queries)
    organic_count = get_players_by_source("organic")
    pubscale_count = get_players_by_source("pubscale")
    timebucks_count = get_players_by_source("timebucks")
    
    # Get latest players (using indexed query)
    latest_players = fetch_latest_players(10)
    
    # Get latest conversions (using optimized approach)
    latest_conversions = fetch_latest_conversions_efficiently(10)

# Display dashboard
st.title("Firebase Game Analytics Dashboard")

# KEY METRICS - Vertical format
st.subheader("Total Number of Players (PLAYERS)")
st.markdown(f"<p class='big-value'>{total_players}</p>", unsafe_allow_html=True)

st.subheader("Total Ad Revenue (PLAYERS)")
st.markdown(f"<p class='big-value'>${total_ad_revenue/100:,.2f}</p>", unsafe_allow_html=True)

st.subheader("Total Impressions (PLAYERS)")
st.markdown(f"<p class='big-value'>{total_impressions:,.0f}</p>", unsafe_allow_html=True)

# SOURCE BREAKDOWN
st.subheader("Source Statistics (PLAYERS)")
st.markdown(f"<p class='big-value'>Number of Organic Players: {organic_count}</p>", unsafe_allow_html=True)
st.markdown(f"<p class='big-value'>Number of Pubscale Players: {pubscale_count}</p>", unsafe_allow_html=True)
st.markdown(f"<p class='big-value'>Number of Timebucks Players: {timebucks_count}</p>", unsafe_allow_html=True)

# --- LATEST PLAYERS SECTION ---
st.header("Latest 10 Players")

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

# Add info about refresh
st.info("Note: This dashboard does not auto-refresh. To see the latest data, refresh the browser page.")

# Add performance note
st.success("This dashboard uses Firebase indexing for optimal performance and reduced database usage.")