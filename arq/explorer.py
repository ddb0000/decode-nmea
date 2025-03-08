import pandas as pd
import argparse
import matplotlib.pyplot as plt
import os
import sys

def load_parquet(filename):
    """Load the parquet file and return a DataFrame."""
    if not os.path.exists(filename):
        print(f"Error: File {filename} not found.")
        sys.exit(1)
    
    try:
        df = pd.read_parquet(filename)
        return df
    except Exception as e:
        print(f"Error loading parquet file: {e}")
        sys.exit(1)

def show_basic_stats(df):
    """Display basic statistics about the dataset."""
    print("\n=== Basic Statistics ===")
    print(f"Total records: {len(df)}")
    print(f"Unique vessels (MMSIs): {df['mmsi'].nunique()}")
    
    # Remove message type stats or replace with vessel type
    if 'type_code' in df.columns:
        type_counts = df['type_code'].value_counts()
        print("\nVessel type distribution:")
        for type_code, count in type_counts.items():
            if pd.notna(type_code):  # Check for NaN
                print(f"  Type {type_code}: {count} records ({count/len(df)*100:.1f}%)")
    
    # Change lat/lon column names
    position_data = df[df['latitude'].notnull()]
    print(f"\nRecords with position data: {len(position_data)} ({len(position_data)/len(df)*100:.1f}%)")
    
    # Change vessel_name to name
    vessel_name_data = df[df['name'].notnull() & (df['name'] != 'N/A')]
    print(f"Records with vessel names: {len(vessel_name_data)} ({len(vessel_name_data)/len(df)*100:.1f}%)")
    
    # Show statistics for vessels with names
    if len(vessel_name_data) > 0:
        print("\nTop 10 vessel names:")
        for name, count in vessel_name_data['name'].value_counts().head(10).items():
            print(f"  {name}: {count}")

def filter_by_mmsi(df, mmsi):
    """Filter data by MMSI number."""
    filtered = df[df['mmsi'] == mmsi]
    print(f"\n=== Data for MMSI {mmsi} ===")
    print(f"Records: {len(filtered)}")
    
    if len(filtered) == 0:
        return filtered
    
    # Update to use your column names
    if 'name' in filtered.columns and pd.notna(filtered['name'].iloc[0]):
        print(f"Vessel name: {filtered['name'].iloc[0]}")
    
    if 'type' in filtered.columns and pd.notna(filtered['type'].iloc[0]):
        print(f"Ship type: {filtered['type'].iloc[0]}")
        
    # Position data uses latitude/longitude instead of lat/lon
    if 'latitude' in filtered.columns and pd.notna(filtered['latitude'].iloc[0]):
        print(f"Position: {filtered['latitude'].iloc[0]:.6f}, {filtered['longitude'].iloc[0]:.6f}")
    
    if 'speed' in filtered.columns and pd.notna(filtered['speed'].iloc[0]):
        print(f"Speed: {filtered['speed'].iloc[0]:.1f} knots")
        
    if 'course' in filtered.columns and pd.notna(filtered['course'].iloc[0]):
        print(f"Course: {filtered['course'].iloc[0]:.1f}°")
        
    return filtered

def filter_by_msg_type(df, msg_type):
    """Filter data by message type."""
    filtered = df[df['msg_type'] == msg_type]
    print(f"\n=== Data for Message Type {msg_type} ===")
    print(f"Records: {len(filtered)}")
    
    if len(filtered) == 0:
        return filtered
    
    unique_mmsi = filtered['mmsi'].nunique()
    print(f"Unique vessels: {unique_mmsi}")
    
    # Display specific information based on message type
    if msg_type in [1, 2, 3, 18]:  # Position reports
        position_data = filtered[filtered['lat'].notnull()]
        if len(position_data) > 0:
            print(f"Valid position reports: {len(position_data)}")
            print(f"Latitude range: {position_data['lat'].min():.6f} to {position_data['lat'].max():.6f}")
            print(f"Longitude range: {position_data['lon'].min():.6f} to {position_data['lon'].max():.6f}")
            print(f"Speed range: {position_data['speed'].min():.1f} to {position_data['speed'].max():.1f} knots")
    
    elif msg_type == 5:  # Static and voyage data
        name_data = filtered[filtered['vessel_name'].notnull() & (filtered['vessel_name'] != 'N/A')]
        if len(name_data) > 0:
            print(f"Vessels with names: {len(name_data)}")
            print("\nSample vessel names:")
            sample = name_data.drop_duplicates('mmsi').head(10)
            for _, row in sample.iterrows():
                print(f"  MMSI {row['mmsi']}: {row['vessel_name']} (Type: {row['ship_type']})")
        else:
            print("No vessels with name information in this message type.")
    
    return filtered

def plot_vessel_track(df, mmsi, save_path=None):
    """Plot vessel track on a map."""
    # Update to use latitude/longitude instead of lat/lon
    position_data = df[(df['mmsi'] == mmsi) & df['latitude'].notnull() & df['longitude'].notnull()]
    
    if len(position_data) == 0:
        print(f"No position data available for MMSI {mmsi}")
        return
        
    title = f"Track for MMSI {mmsi}"
    
    plt.figure(figsize=(10, 8))
    
    vessel_name = "Unknown"
    # Update to use name instead of vessel_name
    vessel_data = df[(df['mmsi'] == mmsi) & (df['name'].notnull()) & (df['name'] != 'N/A')]
    if len(vessel_data) > 0:
        vessel_name = vessel_data.iloc[0]['name']
            
    # Sort by timestamp if available
    if 'last_updated' in position_data.columns and position_data['last_updated'].nunique() > 1:
        position_data = position_data.sort_values('last_updated')
            
    # Update to use longitude/latitude
    plt.plot(position_data['longitude'], position_data['latitude'], '-', label=f"{mmsi} ({vessel_name})")
    plt.plot(position_data['longitude'].iloc[0], position_data['latitude'].iloc[0], 'go', markersize=8)  # Start point
    plt.plot(position_data['longitude'].iloc[-1], position_data['latitude'].iloc[-1], 'ro', markersize=8)  # End point
    
    plt.title(title)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.grid(True)
    plt.legend()
        
    # Adjust plot limits to add a margin around the data
    x_margin = (position_data['longitude'].max() - position_data['longitude'].min()) * 0.05
    y_margin = (position_data['latitude'].max() - position_data['latitude'].min()) * 0.05
    plt.xlim([position_data['longitude'].min() - x_margin, position_data['longitude'].max() + x_margin])
    plt.ylim([position_data['latitude'].min() - y_margin, position_data['latitude'].max() + y_margin])
    
    if save_path:
        plt.savefig(save_path)
        print(f"Plot saved to {save_path}")
    else:
        plt.show()

def list_vessels_with_names(df):
    """List vessels that have names in the dataset."""
    # Change vessel_name to name
    vessel_data = df[df['name'].notnull() & (df['name'] != 'N/A')]
    if len(vessel_data) == 0:
        print("No vessels with name information found.")
        
        # If no names, show top MMSIs instead
        print("\nShowing top 10 vessels by number of messages instead:")
        top_vessels = df['mmsi'].value_counts().head(10)
        for mmsi, count in top_vessels.items():
            print(f"  MMSI: {mmsi}, Messages: {count}")
        return
    
    # Group by MMSI and get the first vessel name for each
    vessels = vessel_data.groupby('mmsi')['name'].first().reset_index()
    
    print(f"\n=== Vessels with Names ({len(vessels)}) ===")
    for i, (_, row) in enumerate(vessels.iterrows(), 1):
        print(f"{i}. MMSI: {row['mmsi']}, Name: {row['name']}")
        if i >= 50:  # Limit display to avoid overwhelming output
            print(f"... and {len(vessels) - 50} more vessels")
            break

def interactive_explore(df):
    """Interactive exploration mode."""
    while True:
        print("\n=== AIS Data Explorer ===")
        print("1. Show basic statistics")
        print("2. List vessels with names")
        print("3. Filter by MMSI")
        print("4. Filter by message type")
        print("5. Plot vessel track")
        print("6. Exit")
        
        choice = input("\nEnter choice (1-6): ")
        
        if choice == '1':
            show_basic_stats(df)
        elif choice == '2':
            list_vessels_with_names(df)
        elif choice == '3':
            try:
                mmsi = int(input("Enter MMSI number: "))
                filtered = filter_by_mmsi(df, mmsi)
            except ValueError:
                print("Invalid MMSI. Please enter a valid number.")
        elif choice == '4':
            try:
                msg_type = int(input("Enter message type (1-27): "))
                if 1 <= msg_type <= 27:
                    filtered = filter_by_msg_type(df, msg_type)
                else:
                    print("Invalid message type. Please enter a number between 1 and 27.")
            except ValueError:
                print("Invalid input. Please enter a valid number.")
        elif choice == '5':
            try:
                mmsi = int(input("Enter MMSI number: "))
                save_path = input("Save plot to file (leave empty to display): ")
                save_path = save_path if save_path.strip() else None
                plot_vessel_track(df, mmsi, save_path)
            except ValueError:
                print("Invalid MMSI. Please enter a valid number.")
        elif choice == '6':
            print("Exiting.")
            break
        else:
            print("Invalid choice, please try again.")

def main():
    parser = argparse.ArgumentParser(description='Explore AIS data from Parquet file')
    parser.add_argument('--file', '-f', default='decoded_ais_data.parquet', help='Parquet file to explore')
    parser.add_argument('--stats', '-s', action='store_true', help='Show basic statistics')
    parser.add_argument('--mmsi', '-m', type=int, help='Filter by MMSI number')
    parser.add_argument('--type', '-t', type=int, help='Filter by message type')
    parser.add_argument('--plot', '-p', action='store_true', help='Plot vessel track for specified MMSI')
    parser.add_argument('--list', '-l', action='store_true', help='List vessels with names')
    parser.add_argument('--output', '-o', help='Save plot to file')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    
    args = parser.parse_args()
    
    print(f"Loading data from {args.file}...")
    df = load_parquet(args.file)
    print(f"Loaded {len(df)} records")
       
    # List available columns:
    print(f"Available columns: {list(df.columns)}")

    # If no specific action is requested, enter interactive mode
    if not (args.stats or args.mmsi or args.type or args.plot or args.list or args.interactive):
        args.interactive = True
    
    if args.interactive:
        interactive_explore(df)
        return
        
    if args.stats:
        show_basic_stats(df)
    
    if args.list:
        list_vessels_with_names(df)
    
    if args.mmsi:
        filtered = filter_by_mmsi(df, args.mmsi)
        # If plotting is requested for a specific MMSI
        if args.plot:
            plot_vessel_track(df, args.mmsi, args.output)
    
    if args.type:
        filter_by_msg_type(df, args.type)

if __name__ == "__main__":
    main()
