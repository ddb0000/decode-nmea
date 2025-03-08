import re
import math
from datetime import datetime
from bitstring import BitArray

def decode_ais(ais_messages):
    results = []
    for message in ais_messages.strip().split('\n'):
        try:
            parts = message.split(',')
            if len(parts) < 6 or not parts[0].startswith('!AIVDM'):
                continue
                
            # Extract relevant parts
            fragment_count = int(parts[1])
            fragment_number = int(parts[2])
            msg_id = parts[3]
            channel = parts[4]
            payload = parts[5]
            timestamp = parts[7] if len(parts) > 7 else None
            
            # Skip multipart messages for simplicity
            if fragment_count > 1:
                continue
                
            # Decode the payload
            decoded = decode_payload(payload)
            if decoded:
                if timestamp:
                    decoded['timestamp'] = timestamp
                results.append(decoded)
        except Exception as e:
            print(f"Error decoding message: {message}\nError: {e}")
            
    return results

def decode_payload(payload):
    # Convert the 6-bit ASCII to binary
    binary = ''
    for c in payload:
        ascii_val = ord(c) - 48
        if ascii_val > 40:
            ascii_val -= 8
        binary += format(ascii_val, '06b')
    
    bits = BitArray(bin=binary)
    
    # Get the message type
    msg_type = bits[0:6].uint
    
    # Decode based on message type
    if msg_type == 1 or msg_type == 2 or msg_type == 3:  # Position reports
        return decode_position_report(bits, msg_type)
    elif msg_type == 4:  # Base station report
        return decode_base_station_report(bits)
    elif msg_type == 5:  # Static and voyage data
        return decode_static_data(bits)
    elif msg_type == 18:  # Class B position report
        return decode_class_b_position(bits)
    elif msg_type == 19:  # Extended Class B position
        return decode_extended_class_b(bits)
    elif msg_type == 21:  # Aid to navigation
        return decode_aid_to_navigation(bits)
    elif msg_type == 24:  # Class B static data
        return decode_class_b_static(bits)
    elif msg_type == 27:  # Long range position report
        return decode_long_range_position(bits)
    else:
        return {
            'msg_type': msg_type,
            'raw': bits.bin,
            'decoded': f"Unsupported message type: {msg_type}"
        }

def decode_position_report(bits, msg_type):
    result = {
        'msg_type': msg_type,
        'msg_description': 'Position Report',
        'mmsi': bits[8:38].uint,
        'navigation_status': bits[38:42].uint,
        'rot': decode_rot(bits[42:50].int),  # Rate of turn
        'sog': bits[50:60].uint / 10.0,  # Speed over ground in knots
        'position_accuracy': bits[60:61].uint,
        'longitude': decode_lon(bits[61:89].int),
        'latitude': decode_lat(bits[89:116].int),
        'cog': bits[116:128].uint / 10.0,  # Course over ground
        'true_heading': bits[128:137].uint,
        'timestamp': bits[137:143].uint,
        'special_manoeuvre': bits[143:145].uint,
        'raim': bits[148:149].uint,  # RAIM flag
    }
    return result

def decode_base_station_report(bits):
    result = {
        'msg_type': 4,
        'msg_description': 'Base Station Report',
        'mmsi': bits[8:38].uint,
        'year': bits[38:52].uint,
        'month': bits[52:56].uint,
        'day': bits[56:61].uint,
        'hour': bits[61:66].uint,
        'minute': bits[66:72].uint,
        'second': bits[72:78].uint,
        'position_accuracy': bits[78:79].uint,
        'longitude': decode_lon(bits[79:107].int),
        'latitude': decode_lat(bits[107:134].int),
        'epfd_type': bits[134:138].uint,  # Electronic Position Fixing Device type
        'raim': bits[148:149].uint,
    }
    return result

def decode_static_data(bits):
    result = {
        'msg_type': 5,
        'msg_description': 'Static and Voyage Data',
        'mmsi': bits[8:38].uint,
        'ais_version': bits[38:40].uint,
        'imo_number': bits[40:70].uint,
        'call_sign': decode_string(bits[70:112]),
        'vessel_name': decode_string(bits[112:232]),
        'ship_type': bits[232:240].uint,
        'dim_to_bow': bits[240:249].uint,
        'dim_to_stern': bits[249:258].uint,
        'dim_to_port': bits[258:264].uint,
        'dim_to_starboard': bits[264:270].uint,
        'epfd_type': bits[270:274].uint,
        'eta_month': bits[274:278].uint,
        'eta_day': bits[278:283].uint,
        'eta_hour': bits[283:288].uint,
        'eta_minute': bits[288:294].uint,
        'draught': bits[294:302].uint / 10.0,  # in meters
        'destination': decode_string(bits[302:422]),
    }
    return result

def decode_class_b_position(bits):
    result = {
        'msg_type': 18,
        'msg_description': 'Class B Position Report',
        'mmsi': bits[8:38].uint,
        'sog': bits[46:56].uint / 10.0,  # Speed over ground
        'position_accuracy': bits[56:57].uint,
        'longitude': decode_lon(bits[57:85].int),
        'latitude': decode_lat(bits[85:112].int),
        'cog': bits[112:124].uint / 10.0,  # Course over ground
        'true_heading': bits[124:133].uint,
        'timestamp': bits[133:139].uint,
    }
    return result

def decode_extended_class_b(bits):
    result = {
        'msg_type': 19,
        'msg_description': 'Extended Class B Position Report',
        'mmsi': bits[8:38].uint,
        'sog': bits[46:56].uint / 10.0,
        'position_accuracy': bits[56:57].uint,
        'longitude': decode_lon(bits[57:85].int),
        'latitude': decode_lat(bits[85:112].int),
        'cog': bits[112:124].uint / 10.0,
        'true_heading': bits[124:133].uint,
        'timestamp': bits[133:139].uint,
        'vessel_name': decode_string(bits[143:263]),
        'ship_type': bits[263:271].uint,
        'dim_to_bow': bits[271:280].uint,
        'dim_to_stern': bits[280:289].uint,
        'dim_to_port': bits[289:295].uint,
        'dim_to_starboard': bits[295:301].uint,
    }
    return result

def decode_aid_to_navigation(bits):
    result = {
        'msg_type': 21,
        'msg_description': 'Aid to Navigation Report',
        'mmsi': bits[8:38].uint,
        'aid_type': bits[38:43].uint,
        'name': decode_string(bits[43:163]),
        'position_accuracy': bits[163:164].uint,
        'longitude': decode_lon(bits[164:192].int),
        'latitude': decode_lat(bits[192:219].int),
        'dim_to_bow': bits[219:228].uint,
        'dim_to_stern': bits[228:237].uint,
        'dim_to_port': bits[237:243].uint,
        'dim_to_starboard': bits[243:249].uint,
        'epfd_type': bits[249:253].uint,
        'timestamp': bits[253:259].uint,
        'off_position': bits[259:260].uint,
        'raim': bits[268:269].uint,
        'virtual_aid': bits[269:270].uint,
    }
    return result

def decode_class_b_static(bits):
    part_number = bits[38:40].uint
    result = {
        'msg_type': 24,
        'msg_description': 'Class B Static Data',
        'mmsi': bits[8:38].uint,
        'part_number': part_number,
    }
    
    if part_number == 0:
        result['vessel_name'] = decode_string(bits[40:160])
    elif part_number == 1:
        result['ship_type'] = bits[40:48].uint
        result['vendor_id'] = decode_string(bits[48:90])
        result['call_sign'] = decode_string(bits[90:132])
        result['dim_to_bow'] = bits[132:141].uint
        result['dim_to_stern'] = bits[141:150].uint
        result['dim_to_port'] = bits[150:156].uint
        result['dim_to_starboard'] = bits[156:162].uint
        
    return result

def decode_long_range_position(bits):
    result = {
        'msg_type': 27,
        'msg_description': 'Long Range Position Report',
        'mmsi': bits[8:38].uint,
        'position_accuracy': bits[38:39].uint,
        'raim': bits[39:40].uint,
        'navigation_status': bits[40:44].uint,
        'longitude': decode_long_range_lon(bits[44:62].int),
        'latitude': decode_long_range_lat(bits[62:79].int),
        'sog': bits[79:85].uint,  # Speed over ground in knots
        'cog': bits[85:94].uint,  # Course over ground
    }
    return result

# Helper functions
def decode_rot(rot_raw):
    if rot_raw == -128:
        return None  # Not available
    if rot_raw == 0:
        return 0  # Not turning
        
    sign = 1 if rot_raw > 0 else -1
    rot_abs = abs(rot_raw)
    return sign * (rot_abs / 4.733) ** 2

def decode_lon(lon_raw):
    if lon_raw == 0x6791AC0:  # 181 degrees = not available
        return None
    return lon_raw / 600000.0

def decode_lat(lat_raw):
    if lat_raw == 0x3412140:  # 91 degrees = not available
        return None
    return lat_raw / 600000.0

def decode_long_range_lon(lon_raw):
    if lon_raw == 0x181:  # 181 degrees = not available
        return None
    return lon_raw / 10.0

def decode_long_range_lat(lat_raw):
    if lat_raw == 0x91:  # 91 degrees = not available
        return None
    return lat_raw / 10.0

def decode_string(bits):
    result = ""
    for i in range(0, len(bits), 6):
        if i + 6 <= len(bits):
            char_code = bits[i:i+6].uint
            if char_code > 0:
                if char_code == 32:  # Space
                    result += ' '
                elif 1 <= char_code <= 31 or 33 <= char_code <= 63:
                    result += chr(char_code + 64)
                elif 64 <= char_code <= 95:
                    result += chr(char_code)
                else:
                    result += '?'
    return result.strip()

# Helper to get navigation status as text
def get_navigation_status(status_code):
    statuses = {
        0: "Under way using engine",
        1: "At anchor",
        2: "Not under command",
        3: "Restricted maneuverability",
        4: "Constrained by her draught",
        5: "Moored",
        6: "Aground",
        7: "Engaged in fishing",
        8: "Under way sailing",
        9: "Reserved for future amendment of navigational status for ships carrying DG, HS, or MP",
        10: "Reserved for future amendment of navigational status for WIG",
        11: "Reserved for future use",
        12: "Reserved for future use",
        13: "Reserved for future use",
        14: "AIS-SART is active",
        15: "Not defined (default)"
    }
    return statuses.get(status_code, "Unknown")

# Helper to get vessel type as text
def get_vessel_type(type_code):
    if 20 <= type_code <= 29:
        return f"Wing in ground (WIG) - {type_code}"
    elif 30 <= type_code <= 39:
        return f"Fishing - {type_code}"
    elif 40 <= type_code <= 49:
        return f"Tug - {type_code}"
    elif 50 <= type_code <= 59:
        return f"Dredger - {type_code}"
    elif 60 <= type_code <= 69:
        return f"Passenger - {type_code}"
    elif 70 <= type_code <= 79:
        return f"Cargo - {type_code}"
    elif 80 <= type_code <= 89:
        return f"Tanker - {type_code}"
    elif 90 <= type_code <= 99:
        return f"Other - {type_code}"
    else:
        return f"Unknown - {type_code}"

# Main function to process the file
def process_ais_file(file_content):
    # Decode all messages
    decoded_messages = decode_ais(file_content)
    
    # Group by MMSI to track vessels
    vessels = {}
    for msg in decoded_messages:
        if 'mmsi' in msg:
            mmsi = msg['mmsi']
            if mmsi not in vessels:
                vessels[mmsi] = []
            vessels[mmsi].append(msg)
    
    # Create a summary
    summary = []
    for mmsi, messages in vessels.items():
        vessel_info = {
            'mmsi': mmsi,
            'message_count': len(messages)
        }
        
        # Try to get vessel name
        for msg in messages:
            if 'vessel_name' in msg and msg['vessel_name']:
                vessel_info['name'] = msg['vessel_name']
                break
        
        # Get latest position
        position_msgs = [msg for msg in messages if 'latitude' in msg and msg['latitude'] is not None]
        if position_msgs:
            latest_pos = position_msgs[-1]
            vessel_info.update({
                'latitude': latest_pos['latitude'],
                'longitude': latest_pos['longitude']
            })
            
            if 'sog' in latest_pos:
                vessel_info['speed'] = latest_pos['sog']
            if 'cog' in latest_pos:
                vessel_info['course'] = latest_pos['cog']
        
        # Get vessel type if available
        for msg in messages:
            if 'ship_type' in msg:
                vessel_info['type'] = get_vessel_type(msg['ship_type'])
                break
        
        summary.append(vessel_info)
    
    return {
        'total_messages': len(decoded_messages),
        'vessel_count': len(vessels),
        'vessels': summary,
        'messages': decoded_messages
    }

# Example usage
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            ais_data = f.read()
    else:
        # Read from stdin
        ais_data = sys.stdin.read()
    
    results = process_ais_file(ais_data)
    
    # Print summary
    print(f"Total messages: {results['total_messages']}")
    print(f"Vessels tracked: {results['vessel_count']}")
    
    print("\nVessel Summary:")
    for vessel in results['vessels']:
        print(f"MMSI: {vessel['mmsi']}")
        if 'name' in vessel:
            print(f"  Name: {vessel['name']}")
        if 'type' in vessel:
            print(f"  Type: {vessel['type']}")
        if 'latitude' in vessel and 'longitude' in vessel:
            print(f"  Position: {vessel['latitude']}, {vessel['longitude']}")
        if 'speed' in vessel:
            print(f"  Speed: {vessel['speed']} knots")
        if 'course' in vessel:
            print(f"  Course: {vessel['course']}°")
        print(f"  Messages: {vessel['message_count']}")
        print("")