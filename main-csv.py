from pyais import decode
import csv

# Decode single or multi-part messages
def decode_message(lines, start_idx, multi_part_buffer):
    line = lines[start_idx].strip()
    parts = line.split(',')
    if len(parts) < 6 or not parts[5]:
        raise ValueError("Empty or malformed payload")
    
    if parts[1] == "2" and parts[2] == "1":  # First part
        seq_id = parts[3]
        if start_idx + 1 < len(lines) and lines[start_idx + 1].split(',')[1] == "2" and lines[start_idx + 1].split(',')[2] == "2" and lines[start_idx + 1].split(',')[3] == seq_id:
            return decode(line.split(',2024-')[0], lines[start_idx + 1].split(',2024-')[0])
        else:
            multi_part_buffer[seq_id] = line
            raise ValueError("Missing second part")
    elif parts[1] == "2" and parts[2] == "2":  # Second part
        seq_id = parts[3]
        if seq_id in multi_part_buffer:
            decoded = decode(multi_part_buffer[seq_id].split(',2024-')[0], line.split(',2024-')[0])
            del multi_part_buffer[seq_id]
            return decoded
        raise ValueError("Missing first part")
    return decode(line.split(',2024-')[0])

# Process a chunk of lines
def process_chunk(chunk, multi_part_buffer, error_log):
    decoded_chunk = []
    i = 0
    while i < len(chunk):
        try:
            decoded = decode_message(chunk, i, multi_part_buffer)
            raw = chunk[i] if "2,1" not in chunk[i] else f"{chunk[i].split(',2024-')[0]} + {chunk[i + 1].split(',2024-')[0]}"
            decoded_chunk.append((decoded, raw))
            i += 2 if "2,1" in chunk[i] else 1
        except Exception as e:
            error_msg = f"Error decoding line {i + 1}: {chunk[i]} - {e}"
            print(error_msg)
            error_log.append(chunk[i])
            i += 1
    return decoded_chunk, multi_part_buffer, error_log

# Write to CSV
def write_to_csv(decoded_list, output_file):
    with open(output_file, 'a', newline='') as csvfile:
        fieldnames = ['msg_type', 'mmsi', 'lat', 'lon', 'speed', 'course', 'vessel_name', 'ship_type', 'timestamp', 'raw']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if csvfile.tell() == 0:
            writer.writeheader()
        for decoded, raw in decoded_list:
            msg_type = decoded.msg_type
            mmsi = decoded.mmsi
            timestamp = raw.split(',2024-')[1] if ',2024-' in raw else 'N/A'
            row = {
                'msg_type': msg_type,
                'mmsi': mmsi,
                'lat': decoded.lat if msg_type in [1, 2, 3, 18] else None,
                'lon': decoded.lon if msg_type in [1, 2, 3, 18] else None,
                'speed': decoded.speed if msg_type in [1, 2, 3, 18] else None,
                'course': decoded.course if msg_type in [1, 2, 3, 18] else None,
                'vessel_name': getattr(decoded, 'name', 'N/A') if msg_type == 5 else None,
                'ship_type': getattr(decoded, 'ship_type', 'N/A') if msg_type == 5 else None,
                'timestamp': timestamp,
                'raw': raw
            }
            writer.writerow(row)

# Summarize decoded messages
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
        if msg_type in [1, 2, 3, 18]:
            print(f"  Lat: {decoded.lat}, Lon: {decoded.lon}, Speed: {decoded.speed}")
        elif msg_type == 5:
            print(f"  Vessel Name: {getattr(decoded, 'name', 'N/A')}, Ship Type: {getattr(decoded, 'ship_type', 'N/A')}")
    print(f"Message Types: {msg_types}")
    print(f"Unique MMSIs: {len(mmsi_set)}")

# Main processing
filename = "20240911_06053.txt"
output_file = "decoded_ais_data.csv"
error_log_file = "ais_errors.txt"
chunk_size = 2000  # Increased to reduce split multi-part messages
multi_part_buffer = {}
error_log = []

with open(filename, 'r') as f:
    lines = [line.strip() for line in f.readlines() if line.strip()]
    total_lines = len(lines)
    print(f"Total lines in file: {total_lines}")

    all_decoded = []
    for start in range(0, total_lines, chunk_size):
        end = min(start + chunk_size, total_lines)
        chunk = lines[start:end]
        decoded_chunk, multi_part_buffer, error_log = process_chunk(chunk, multi_part_buffer, error_log)
        all_decoded.extend(decoded_chunk)
        write_to_csv(decoded_chunk, output_file)
        
        if start == 0:
            summarize_decoded(decoded_chunk[:min(30, len(decoded_chunk))], "First 30 Lines")
        elif start <= total_lines // 2 < end:
            middle_start = max(0, (total_lines // 2) - start - 5)
            middle_end = min(middle_start + 10, len(decoded_chunk))
            summarize_decoded(decoded_chunk[middle_start:middle_end], "Middle Sample")
        if end == total_lines:
            summarize_decoded(decoded_chunk[-min(20, len(decoded_chunk)):], "Last 20 Lines")

# Write errors to log file
with open(error_log_file, 'w') as f:
    f.write("\n".join(error_log))

print(f"Decoding complete. Results saved to {output_file}. Errors logged to {error_log_file}")