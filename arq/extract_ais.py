import pyais #add this line
def ascii_to_bit_string(ascii_str):
    """Converts ASCII characters to a bit string."""
    bit_string = ""
    for char in ascii_str:
        value = ord(char) - 48 if ord(char) < 96 else ord(char) - 88
        bit_string += bin(value)[2:].zfill(6)
    return bit_string

def extract_message_type(ais_line):
    """Extracts the AIS message type from a line."""
    if not ais_line.startswith('!AIVDM'):
        return None

    try:
        parts = ais_line.split(',')
        payload = parts[5]
        bit_string = ascii_to_bit_string(payload)
        message_type = int(bit_string[:6], 2)
        return message_type
    except (ValueError, IndexError):
        return None

def find_unique_message_types(filename):
    """Finds unique AIS message types from a file."""
    unique_types = set()
    try:
        with open(filename, 'r') as file:
            for line in file:
                message_type = extract_message_type(line)
                if message_type is not None:
                    unique_types.add(message_type)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return set()
    return sorted(list(unique_types))

# Example Usage
filename = '20240911_06053.txt'
unique_message_types = find_unique_message_types(filename)

if unique_message_types:
    print(f"Unique AIS message types found: {unique_message_types}")

def count_message_types(filename):
    """Counts the occurrences of each AIS message type."""
    message_type_counts = {}
    try:
        with open(filename, 'r') as file:
            for line in file:
                message_type = extract_message_type(line)
                if message_type is not None:
                    if message_type in message_type_counts:
                        message_type_counts[message_type] += 1
                    else:
                        message_type_counts[message_type] = 1
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return {}
    return dict(sorted(message_type_counts.items()))

# Example usage
filename = '20240911_06053.txt'
type_counts = count_message_types(filename)

if type_counts:
    print("AIS Message Type Counts:")
    for message_type, count in type_counts.items():
        print(f"Type {message_type}: {count}")

def inspect_message_type(filename, target_type, num_examples=5):
    """Inspects messages of a specific type."""
    count = 0
    try:
        with open(filename, 'r') as file:
            for line in file:
                message_type = extract_message_type(line)
                if message_type == target_type:
                    try:
                        decoded_message = pyais.decode(line)
                        if isinstance(decoded_message, list):
                            for msg in decoded_message:
                                print(f"Decoded Message (Type {target_type}): {msg}")
                                count += 1
                                if count >= num_examples:
                                    return
                    except pyais.exceptions.ParseError as e:
                        print(f"Error decoding line: {line.strip()}. Error: {e}")
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")

# Example usage (inspect type 1 and 5)
inspect_message_type(filename, 1)
inspect_message_type(filename, 5)