# AIS Message Types Reference Guide

AIS messages are divided into different types, each containing specific information. Below is a reference for the most common types found in your data.

## Common Message Types

| Msg Type | Description | Common Fields | Notes |
|----------|-------------|--------------|-------|
| 1, 2, 3 | Position Report (Class A) | mmsi, lat, lon, speed, course | Basic vessel position updates |
| 5 | Static and Voyage Data | mmsi, vessel_name, ship_type | Contains vessel name, callsign, dimensions |
| 18 | Position Report (Class B) | mmsi, lat, lon, speed, course | Similar to types 1-3 but for Class B transponders |
| 24 | Static Data Report (Class B) | mmsi, vessel_name | May contain vessel name in part A |
| 21 | Aid to Navigation | mmsi, name, lat, lon | For buoys, lighthouses, etc. |

## Why Vessel Names Are Missing

1. **Type 5 messages are infrequent**: Vessels typically broadcast type 5 messages much less frequently than position updates (every 6 minutes vs. every few seconds for position)

2. **Message distribution in your data**:
   - Most of your messages are types 1, 3, and 18 (position reports)
   - Very few type 5 messages (containing vessel names)

3. **Data collection duration**: To capture vessel names, you typically need to collect data for longer periods (hours rather than minutes)

## Ship Type Codes (for Type 5 messages)

The `ship_type` field contains a numeric code:

| Code Range | Vessel Type |
|------------|-------------|
| 20-29 | Wing in ground craft |
| 30-39 | Fishing vessels |
| 40-49 | High-speed craft |
| 50-59 | Special craft (pilots, tugs, etc.) |
| 60-69 | Passenger vessels |
| 70-79 | Cargo vessels |
| 80-89 | Tankers |
| 90-99 | Other types |

## Improving Vessel Name Capture

To increase the number of vessels with names:

1. **Collect data for longer periods** (several hours to days)

2. **Filter by message type 5** (use `--type 5` in explorer.py)

3. **Try matching MMSIs across datasets** if you have access to vessel databases
