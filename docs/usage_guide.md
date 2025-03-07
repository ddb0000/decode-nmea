# Using the AIS Decoder Tools

This guide explains how to use the different AIS decoder tools in this repository.

## 1. Setup Environment

First, make sure you have the required dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install pyais pandas pyarrow matplotlib
```

Or if you're using Nix:

```bash
nix-shell
```

## 2. Basic Decoding (main-parquet.py)

For basic decoding that saves all messages to a parquet file:

```bash
python main-parquet.py
```

This will:
- Read AIS messages from `20240911_06053.txt`
- Decode them into a structured format
- Save the results to `decoded_ais_data.parquet`
- Log errors to `ais_errors.txt`

## 3. Enhanced Vessel-Centric Decoding (ais_decoder_improvements.py)

For improved decoding that consolidates vessel information across messages:

```bash
python ais_decoder_improvements.py --input 20240911_06053.txt
```

Options:
- `--input` or `-i`: Specify the input file with AIS messages (required)
- `--output` or `-o`: Specify the output parquet file (optional, defaults to `[input-filename]_vessels.parquet`)

Example with custom output file:
```bash
python ais_decoder_improvements.py --input 20240911_06053.txt --output vessels_database.parquet
```

### Key Benefits of Improved Decoder:

1. **Vessel-centric approach**: Creates one row per vessel instead of one row per message
2. **Better name matching**: Preserves vessel names across messages for the same MMSI
3. **Human-readable ship types**: Converts numeric codes to descriptive vessel types
4. **Statistics**: Shows how many vessels have names, positions, etc.


### Explorer Features:

- **Interactive mode** (default): Menu-driven interface
- **Statistics**: `--stats` or `-s` flag shows dataset statistics
- **Filter by MMSI**: `--mmsi 123456789` shows data for a specific vessel
- **Filter by message type**: `--type 5` shows all type 5 messages
- **Plot vessel track**: `--mmsi 123456789 --plot` visualizes a vessel's movement
- **List vessels**: `--list` shows vessels with names

Examples:
```bash
# Show statistics
python explorer.py --stats

# Show all type 5 messages (with vessel names)
python explorer.py --type 5

# Plot a specific vessel's track
python explorer.py --mmsi 563121300 --plot

# Save plot to file
python explorer.py --mmsi 563121300 --plot --output vessel_track.png
```