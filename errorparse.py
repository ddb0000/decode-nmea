"""
### WIP ###
This script reads an AIS error log file and processes the errors.
It decodes multi-part Type 5 messages and writes the decoded messages to a CSV.
It writes the remaining proprietary messages to a text file.

Idea for processing errors:
- Read the error log file
- For each line:
    - If it's a multi-part message, store the first part in a buffer
    - If it's the second part, decode the message and store it
    - If it's not a multi-part message, store it as proprietary
- Write the decoded messages to a CSV
- Write the proprietary messages to a text file
"""
import csv

def decode_type5_raw(payload1, payload2):
    combined = payload1 + payload2
    bits = ''.join(format(ord(c) - 48 if c < 'W' else ord(c) - 56, '06b') for c in combined)
    mmsi = int(bits[0:30], 2)
    name = ''.join(chr(int(bits[i:i+6], 2) + 64) for i in range(8, 128, 6)).strip('@ ')
    ship_type = int(bits[128:136], 2)
    return {'mmsi': mmsi, 'name': name, 'ship_type': ship_type}

def process_errors(error_file, output_csv):
    multi_part_buffer = {}
    decoded_errors = []
    proprietary = []

    with open(error_file, 'r') as f:
        lines = [line.strip() for line in f.readlines()]
    
    for line in lines:
        parts = line.split(',')
        if len(parts) < 6 or not parts[5]:  # Empty payload
            continue
        if parts[1] == "2" and parts[2] == "1":  # First part
            seq_id = f"{parts[3]}_{parts[4]}"
            multi_part_buffer[seq_id] = line
        elif parts[1] == "2" and parts[2] == "2":  # Second part
            seq_id = f"{parts[3]}_{parts[4]}"
            if seq_id in multi_part_buffer:
                try:
                    p1 = multi_part_buffer[seq_id].split(',2024-')[0].split(',')[5]
                    p2 = line.split(',2024-')[0].split(',')[5]
                    decoded = decode_type5_raw(p1, p2)
                    decoded_errors.append({
                        'msg_type': 5,
                        'mmsi': decoded['mmsi'],
                        'vessel_name': decoded['name'],
                        'ship_type': decoded['ship_type'],
                        'timestamp': line.split(',2024-')[1],
                        'raw': f"{multi_part_buffer[seq_id].split(',2024-')[0]} + {line.split(',2024-')[0]}"
                    })
                    del multi_part_buffer[seq_id]
                except Exception as e:
                    print(f"Failed to decode Type 5: {line} - {e}")
        elif not line.startswith('!AIVDM'):  # Proprietary?
            proprietary.append(line)
        else:
            proprietary.append(line)  # Unhandled single-part messages

    # Write decoded errors to CSV
    with open(output_csv, 'w', newline='') as csvfile:
        fieldnames = ['msg_type', 'mmsi', 'lat', 'lon', 'speed', 'course', 'vessel_name', 'ship_type', 'timestamp', 'raw']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in decoded_errors:
            writer.writerow(row)

    # Log remaining proprietary messages
    with open('proprietary_errors.txt', 'w') as f:
        f.write("\n".join(proprietary))

    print(f"Processed {len(lines)} errors. Decoded {len(decoded_errors)} Type 5 messages to {output_csv}. {len(proprietary)} proprietary/undecoded in proprietary_errors.txt")
    if multi_part_buffer:
        print(f"Remaining unpaired Type 5 first parts: {len(multi_part_buffer)}")

# Run it
process_errors("ais_errors.txt", "decoded_errors.csv")