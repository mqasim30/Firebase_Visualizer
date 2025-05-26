import os
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
from datetime import datetime, timedelta

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

# Function to fetch a specific player by UID
def fetch_player(uid):
    try:
        ref = database.reference(f"PLAYERS/{uid}")
        data = ref.get()
        if data and isinstance(data, dict):
            return data
        return None
    except Exception as e:
        logging.error(f"Error fetching player {uid}: {e}")
        return None

# Function to fetch the latest 10 conversions efficiently with player data
def fetch_latest_conversions_with_player_data(limit=10):
    try:
        # Directly get the entire CONVERSIONS branch
        conv_ref = database.reference("CONVERSIONS")
        all_data = conv_ref.get()
        
        if not all_data or not isinstance(all_data, dict):
            logging.warning("No conversion data found")
            return []
            
        # Flatten the nested structure
        all_conversions = []
        
        # Process the nested structure
        for user_id, user_data in all_data.items():
            if not isinstance(user_data, dict):
                continue
                
            for conv_id, conv_data in user_data.items():
                if not isinstance(conv_data, dict):
                    continue
                    
                # Create a record with all the relevant fields
                conversion = {
                    "user_id": user_id,
                    "conversion_id": conv_id,
                    **conv_data  # This adds goal, source, time
                }
                all_conversions.append(conversion)
        
        # Sort by time (descending) and take the latest ones
        sorted_conversions = sorted(
            all_conversions, 
            key=lambda x: x.get("time", 0), 
            reverse=True
        )
        
        # Take only the requested number
        latest_conversions = sorted_conversions[:limit]
        
        # Enhance each conversion with player data
        enhanced_conversions = []
        for conversion in latest_conversions:
            user_id = conversion.get("user_id")
            
            # Fetch player data directly using the user_id
            player_data = fetch_player(user_id)
            
            if player_data:
                # Add player data as prefixed fields (to avoid name collisions)
                player_fields = {
                    "player_geo": player_data.get("Geo", ""),
                    "player_source": player_data.get("Source", ""),
                    "player_ip": player_data.get("IP", ""),
                    "player_wins": player_data.get("Wins", 0),
                    "player_impressions": player_data.get("Impressions", 0),
                    "player_ad_revenue": player_data.get("Ad_Revenue", 0),
                    "player_install_time": player_data.get("Install_time", 0),
                    "player_last_impression_time": player_data.get("Last_Impression_time", 0)
                }
                
                # Combine conversion and player data
                enhanced_conversion = {**conversion, **player_fields}
                enhanced_conversions.append(enhanced_conversion)
            else:
                # If player data not found, just use the conversion data
                enhanced_conversions.append(conversion)
        
        logging.info(f"Found {len(all_conversions)} total conversions, returning {len(enhanced_conversions)} enhanced conversions")
        
        return enhanced_conversions
        
    except Exception as e:
        logging.error(f"Error fetching conversions with player data: {e}")
        return []

# Function to fetch the latest 10 IAP purchases efficiently with player data
def fetch_latest_iap_with_player_data(limit=10):
    try:
        # Directly get the entire IAP branch
        iap_ref = database.reference("IAP")
        all_data = iap_ref.get()
        
        # Add debug logging to see the raw data structure
        logging.info("Raw IAP data structure: %s", str(all_data)[:200] + "..." if all_data else "None")
        
        if not all_data or not isinstance(all_data, dict):
            logging.warning("No IAP data found or invalid data structure")
            return []
            
        # Flatten the nested structure
        all_iaps = []
        
        # Process the nested structure - from the screenshot we can see the exact structure
        for user_id, user_data in all_data.items():
            if not isinstance(user_data, dict):
                logging.warning(f"User data for {user_id} is not a dict: {type(user_data)}")
                continue
                
            # Debug log to see user_data structure
            logging.info(f"User {user_id} has {len(user_data)} IAP records")
                
            for purchase_id, purchase_data in user_data.items():
                if not isinstance(purchase_data, dict):
                    logging.warning(f"Purchase data for {purchase_id} is not a dict: {type(purchase_data)}")
                    continue
                
                # Debug log to see purchase_data structure
                logging.info(f"Purchase {purchase_id} data: {purchase_data}")
                    
                # Create a record with all the relevant fields
                iap = {
                    "user_id": user_id,
                    "purchase_id": purchase_id,
                    **purchase_data  # This adds name, price, timeBought
                }
                all_iaps.append(iap)
        
        logging.info(f"Total IAP records collected: {len(all_iaps)}")
        
        if not all_iaps:
            logging.warning("No IAP records were collected after processing the data")
            return []
        
        # Sort by timeBought (descending) and take the latest ones
        try:
            sorted_iaps = sorted(
                all_iaps, 
                key=lambda x: x.get("timeBought", 0), 
                reverse=True
            )
            logging.info(f"Successfully sorted {len(sorted_iaps)} IAP records")
        except Exception as e:
            logging.error(f"Error sorting IAP data: {e}")
            # If sorting fails, just use the unsorted list
            sorted_iaps = all_iaps
        
        # Take only the requested number
        latest_iaps = sorted_iaps[:limit]
        logging.info(f"Selected {len(latest_iaps)} latest IAP records")
        
        # Enhance each IAP with player data
        enhanced_iaps = []
        for iap in latest_iaps:
            user_id = iap.get("user_id")
            
            # Fetch player data directly using the user_id
            player_data = fetch_player(user_id)
            
            if player_data:
                # Add player data as prefixed fields (to avoid name collisions)
                player_fields = {
                    "player_geo": player_data.get("Geo", ""),
                    "player_source": player_data.get("Source", ""),
                    "player_ip": player_data.get("IP", ""),
                    "player_wins": player_data.get("Wins", 0),
                    "player_impressions": player_data.get("Impressions", 0),
                    "player_ad_revenue": player_data.get("Ad_Revenue", 0),
                    "player_install_time": player_data.get("Install_time", 0),
                    "player_last_impression_time": player_data.get("Last_Impression_time", 0)
                }
                
                # Combine IAP and player data
                enhanced_iap = {**iap, **player_fields}
                enhanced_iaps.append(enhanced_iap)
            else:
                # If player data not found, just use the IAP data
                enhanced_iaps.append(iap)
        
        logging.info(f"Returning {len(enhanced_iaps)} enhanced IAP records")
        
        # Debug: Log the first record to check its structure
        if enhanced_iaps:
            logging.info(f"Sample IAP record: {enhanced_iaps[0]}")
        
        return enhanced_iaps
        
    except Exception as e:
        logging.error(f"Error fetching IAP purchases with player data: {e}")
        # Add more detailed error information including trace
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return []

def format_timestamp(timestamp):
    if pd.notna(timestamp) and timestamp != 0:
        try:
            # Convert to datetime
            dt = datetime.fromtimestamp(timestamp/1000)
            # Add 5 hours to adjust for timezone
            dt = dt + timedelta(hours=5)
            return dt.strftime('%H:%M:%S %Y-%m-%d')
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
    
    if "Last_Impression_time" in latest_df.columns:
        latest_df["Last_Impression_time"] = latest_df["Last_Impression_time"].apply(format_timestamp)
    
    # Display key information in a clean table
    display_cols = ["uid", "Formatted_Install_time", "Source", "Geo", "IP", "Wins", "Goal", "Impressions", "Ad_Revenue", "Last_Impression_time"]
    display_cols = [col for col in display_cols if col in latest_df.columns]
    
    st.dataframe(latest_df[display_cols])

# --- LATEST CONVERSIONS SECTION WITH PLAYER DATA ---
st.header("Latest 10 Conversions (With Player Data)")

with st.spinner("Loading latest conversions with player data..."):
    latest_conversions = fetch_latest_conversions_with_player_data(10)

if not latest_conversions:
    st.warning("No conversions found. Make sure your CONVERSIONS data is properly structured.")
else:
    # Create DataFrame from the enhanced conversions data
    conversions_df = pd.DataFrame(latest_conversions)
    
    # Format the timestamps to be more readable
    if "time" in conversions_df.columns:
        conversions_df["Formatted_time"] = conversions_df["time"].apply(format_timestamp)
    
    if "player_install_time" in conversions_df.columns:
        conversions_df["Formatted_install_time"] = conversions_df["player_install_time"].apply(format_timestamp)
        
    if "player_last_impression_time" in conversions_df.columns:
        conversions_df["Formatted_last_impression_time"] = conversions_df["player_last_impression_time"].apply(format_timestamp)
    
    # Display the conversion information with player data
    display_cols = [
        "user_id", "conversion_id", "Formatted_time", "goal", "source",
        "player_source", "player_geo", "player_ip", "player_wins", 
        "player_impressions", "player_ad_revenue", "Formatted_install_time", "Formatted_last_impression_time"
    ]
    display_cols = [col for col in display_cols if col in conversions_df.columns]
    
    st.dataframe(conversions_df[display_cols])

# --- LATEST IAP PURCHASES SECTION WITH PLAYER DATA ---
st.header("Latest 10 In-App Purchases (With Player Data)")

with st.spinner("Loading latest IAP purchases with player data..."):
    latest_iaps = fetch_latest_iap_with_player_data(10)

# Add debug display to see if any data was returned
st.write(f"Found {len(latest_iaps)} IAP records")

if not latest_iaps:
    st.warning("No IAP purchases found. Make sure your IAP data is properly structured.")
else:
    # Create DataFrame from the enhanced IAP data
    iaps_df = pd.DataFrame(latest_iaps)
    
    # Format the timestamps to be more readable
    if "timeBought" in iaps_df.columns:
        iaps_df["Formatted_time_bought"] = iaps_df["timeBought"].apply(format_timestamp)
    
    if "player_install_time" in iaps_df.columns:
        iaps_df["Formatted_install_time"] = iaps_df["player_install_time"].apply(format_timestamp)
        
    if "player_last_impression_time" in iaps_df.columns:
        iaps_df["Formatted_last_impression_time"] = iaps_df["player_last_impression_time"].apply(format_timestamp)
    
    # Display the IAP information with player data
    display_cols = [
        "user_id", "purchase_id", "name", "price", "Formatted_time_bought",
        "player_source", "player_geo", "player_ip", "player_wins", 
        "player_impressions", "player_ad_revenue", "Formatted_install_time", "Formatted_last_impression_time"
    ]
    display_cols = [col for col in display_cols if col in iaps_df.columns]
    
    st.dataframe(iaps_df[display_cols])