from pyais.stream import FileReaderStream
import logging

# Configure logging to write to a file
logging.basicConfig(
    filename='ais_decode_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

filename = "20240911_06053.txt"

# Dictionary to store decoded data
decoded_data = {
    "ship_names": {},  # MMSI -> ship name
    "positions": [],   # List of (MMSI, lat, lon)
    "statuses": {},    # MMSI -> navigation status
    "speeds": {},      # MMSI -> speed
    "courses": {},     # MMSI -> course
}

successful_count = 0
error_count = 0

with FileReaderStream(filename) as stream:
    for msg in stream:
        try:
            decoded = msg.decode()
            successful_count += 1
            print(decoded)  # Print each decoded message as it processes

            # Extract fields based on message type
            mmsi = getattr(decoded, "mmsi", None)
            if not mmsi:
                continue

            # Message Type 24 Part A (ship name)
            if hasattr(decoded, "shipname"):
                decoded_data["ship_names"][mmsi] = decoded.shipname.strip()

            # Message Type 1, 2, 3 (position reports)
            if hasattr(decoded, "lat") and hasattr(decoded, "lon"):
                if decoded.lat != 91.0 and decoded.lon != 181.0:  # Check for default "unavailable" values
                    decoded_data["positions"].append((mmsi, decoded.lat, decoded.lon))
                if hasattr(decoded, "status"):
                    decoded_data["statuses"][mmsi] = str(decoded.status)
                if hasattr(decoded, "speed"):
                    decoded_data["speeds"][mmsi] = decoded.speed
                if hasattr(decoded, "course"):
                    decoded_data["courses"][mmsi] = decoded.course

        except Exception as e:
            error_count += 1
            logging.error(f"Failed to decode message: {msg.raw} - Error: {str(e)}")
            print(f"Skipping invalid message: {msg.raw} - Error: {str(e)}")

# Print summary after processing
print("\n=== AIS Decoding Summary ===")
print(f"Successfully decoded messages: {successful_count}")
print(f"Messages with errors: {error_count}")
print("\nShip Names:")
for mmsi, name in decoded_data["ship_names"].items():
    print(f"  MMSI: {mmsi} - Ship Name: {name}")
