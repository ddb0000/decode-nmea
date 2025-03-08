from pyais.stream import FileReaderStream
import logging
import pandas as pd

# Configure logging to write to a file
logging.basicConfig(
    filename='ais_decode_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

filename = "20240911_06053.txt"
output_parquet = "ais_data_20240911.parquet"

# Lists to store data for DataFrame
ship_data = []  # For static data (ship names, call signs)
position_data = []  # For dynamic data (positions, speeds, etc.)

successful_count = 0
error_count = 0

with FileReaderStream(filename) as stream:
    for msg in stream:
        try:
            decoded = msg.decode()
            successful_count += 1
            print(decoded)  # Print each decoded message as it processes

            mmsi = getattr(decoded, "mmsi", None)
            if not mmsi:
                continue

            # Extract timestamp from raw message
            raw_msg = str(msg.raw)
            timestamp = raw_msg.split(',')[-1] if ',' in raw_msg else None

            # Message Type 24 Part A (ship name) and Part B (call sign)
            if hasattr(decoded, "shipname"):
                ship_data.append({
                    "mmsi": mmsi,
                    "vessel_name": decoded.shipname.strip(),
                    "call_sign": getattr(decoded, "callsign", None),
                    "timestamp": timestamp
                })

            # Message Type 1, 2, 3, 18 (position reports)
            if hasattr(decoded, "lat") and hasattr(decoded, "lon"):
                position_data.append({
                    "mmsi": mmsi,
                    "lat": decoded.lat if decoded.lat != 91.0 else None,
                    "lon": decoded.lon if decoded.lon != 181.0 else None,
                    "sog": getattr(decoded, "speed", None),
                    "cog": getattr(decoded, "course", None),
                    "nas": str(getattr(decoded, "status", None)) if hasattr(decoded, "status") else None,
                    "heading": getattr(decoded, "heading", None),
                    "rot": getattr(decoded, "turn", None),  # Rate of turn
                    "mi": getattr(decoded, "maneuver", None),  # Maneuver indicator
                    "timestamp": timestamp
                })

        except Exception as e:
            error_count += 1
            logging.error(f"Failed to decode message: {msg.raw} - Error: {str(e)}")
            print(f"Skipping invalid message: {msg.raw} - Error: {str(e)}")

# Convert to DataFrames
ship_df = pd.DataFrame(ship_data)
position_df = pd.DataFrame(position_data)

# Merge ship names and call signs into position data
if not ship_df.empty and not position_df.empty:
    combined_df = position_df.merge(
        ship_df[["mmsi", "vessel_name", "call_sign"]].drop_duplicates(),
        on="mmsi",
        how="left"
    )
else:
    combined_df = position_df

# Add IMO column (set to None since we don't have MessageType5)
combined_df["imo"] = None

# Rename timestamp to time *before* reordering columns
combined_df = combined_df.rename(columns={"timestamp": "time"})

# Reorder columns to match DE schema, including lat and lon
combined_df = combined_df[[
    "mmsi", "vessel_name", "time", "lat", "lon", "heading", "rot", "sog", "cog", "nas", "mi", "imo", "call_sign"
]]

# Ensure consistent data types
combined_df["mmsi"] = combined_df["mmsi"].astype("int64")
combined_df["vessel_name"] = combined_df["vessel_name"].fillna("Unknown").astype("string")
combined_df["time"] = pd.to_datetime(combined_df["time"], errors="coerce")
combined_df["lat"] = combined_df["lat"].astype("float64")
combined_df["lon"] = combined_df["lon"].astype("float64")
combined_df["heading"] = combined_df["heading"].astype("float64")
combined_df["rot"] = combined_df["rot"].astype("float64")
combined_df["sog"] = combined_df["sog"].astype("float64")
combined_df["cog"] = combined_df["cog"].astype("float64")
combined_df["nas"] = combined_df["nas"].fillna("Unknown").astype("string")
combined_df["mi"] = combined_df["mi"].astype("Int64")  # Nullable integer
combined_df["imo"] = combined_df["imo"].astype("string")
combined_df["call_sign"] = combined_df["call_sign"].fillna("").astype("string")

# Save to Parquet
combined_df.to_parquet(output_parquet, index=False, engine="pyarrow")

# Print summary
print("\n=== AIS Decoding Summary ===")
print(f"Successfully decoded messages: {successful_count}")
print(f"Messages with errors: {error_count}")
print(f"Data saved to: {output_parquet}")
print("\nSample of the combined data:")
print(combined_df.head())
print("\nShip Names (unique):")
unique_ships = combined_df[["mmsi", "vessel_name"]].drop_duplicates().sort_values("mmsi")
for _, row in unique_ships.iterrows():
    print(f"  MMSI: {row['mmsi']} - Ship Name: {row['vessel_name']}")