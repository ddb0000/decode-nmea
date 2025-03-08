from pyais import decode

# Function to decode single or multi-part messages
def decode_message(lines, start_idx):
    if "2,1" in lines[start_idx]:  # Multi-part message
        return decode(lines[start_idx].split(',2024-')[0], lines[start_idx + 1].split(',2024-')[0])
    return decode(lines[start_idx].split(',2024-')[0])

# Mock
data = [
    "!AIVDM,1,1,,B,1815UdP000Ld<0aj<F76cC5f0@Q6,0*49,2024-09-11 00:00:01",
    "!AIVDM,1,1,,A,1:U79tHP00LcesOjEcbWdOwf26sd,0*3C,2024-09-11 00:00:01",
    "!AIVDM,1,1,,B,1:U90NPP00td51wjAAf=Kgwh00Rd,0*6B,2024-09-11 23:58:59",
    "!AIVDM,1,1,,B,B08el30007;1a@LTHEdhpWM00<00,0*36,2024-09-11 23:59:01",
    "!AIVDM,1,1,,B,B:U7EVh00;?8mP=18D3Q3wwP2h06,0*68,2024-09-12 00:00:01"
]

# Decode first 30 lines
decoded_first = []
for i in range(min(30, len(data))):
    if i < len(data) - 1 and "2,1" in data[i]:  # Multi-part
        decoded = decode_message(data, i)
        decoded_first.append((decoded, data[i], data[i + 1]))
        i += 1  # Skip the second part
    else:
        decoded = decode_message(data, i)
        decoded_first.append((decoded, data[i]))

# Decode middle sample (around 00:01:01, lines 50-60)
middle_idx = 50  # Approximate middle
decoded_middle = []
for i in range(middle_idx, min(middle_idx + 10, len(data))):
    if i < len(data) - 1 and "2,1" in data[i]:
        decoded = decode_message(data, i)
        decoded_middle.append((decoded, data[i], data[i + 1]))
        i += 1
    else:
        decoded = decode_message(data, i)
        decoded_middle.append((decoded, data[i]))

# Decode last 20 lines
end_idx = max(0, len(data) - 20)
decoded_end = []
for i in range(end_idx, len(data)):
    if i < len(data) - 1 and "2,1" in data[i]:
        decoded = decode_message(data, i)
        decoded_end.append((decoded, data[i], data[i + 1]))
        i += 1
    else:
        decoded = decode_message(data, i)
        decoded_end.append((decoded, data[i]))

# Summarize results
def summarize_decoded(decoded_list, section):
    print(f"\n=== {section} ===")
    msg_types = {}
    mmsi_set = set()
    for decoded, raw in decoded_list:
        msg_type = decoded.msg_type
        mmsi = decoded.mmsi
        msg_types[msg_type] = msg_types.get(msg_type, 0) + 1
        mmsi_set.add(mmsi)
        print(f"Type: {msg_type}, MMSI: {mmsi}, Raw: {raw.split(',2024-')[0]}")
        if msg_type in [1, 2, 3]:
            print(f"  Lat: {decoded.lat}, Lon: {decoded.lon}, Speed: {decoded.speed}")
        elif msg_type == 5:
            print(f"  Vessel Name: {decoded.name}, Ship Type: {decoded.ship_type}")
    print(f"Message Types: {msg_types}")
    print(f"Unique MMSIs: {len(mmsi_set)}")

summarize_decoded(decoded_first, "First 30 Lines")
summarize_decoded(decoded_middle, "Middle Sample (00:01:01)")
summarize_decoded(decoded_end, "Last 20 Lines")