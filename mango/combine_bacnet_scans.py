import os
import glob
import pandas as pd

def validate_bacnet_files(commands_path, folder_path):
    """Validate that each BACnet ID in commands spreadsheet has a corresponding file."""
    if not os.path.isfile(commands_path):
        print(f"Error: Commands file not found -> {commands_path}")
        return False, []

    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found -> {folder_path}")
        return False, []

    try:
        df_commands = pd.read_excel(commands_path)
    except Exception as e:
        print(f"Error reading commands Excel file: {e}")
        return False, []

    if "BACnet ID" not in df_commands.columns:
        print("Error: 'BACnet ID' column not found in commands spreadsheet.")
        return False, []

    bacnet_ids = df_commands["BACnet ID"].dropna().astype(int).astype(str).unique()
    folder_files = os.listdir(folder_path)
    missing_ids = []

    for device_id in bacnet_ids:
        expected_filename = f"bacnet-scan_{device_id}.xlsx"
        if expected_filename not in folder_files:
            missing_ids.append(device_id)

    if missing_ids:
        print("❌ Missing files for the following BACnet IDs:")
        for device_id in missing_ids:
            print(f"  {device_id}")
        return False, []
    
    print("✅ All BACnet IDs have corresponding files.")
    return True, bacnet_ids

def process_bacnet_files(folder_path, bacnet_ids):
    """Combine devices tabs and copy individual device tabs into bacnet-scan.xlsx."""
    file_pattern = os.path.join(folder_path, "bacnet-scan_*.xlsx")
    files = glob.glob(file_pattern)

    if not files:
        print("No files found matching bacnet-scan_*.xlsx in the folder.")
        return

    all_devices_df = []
    device_tabs_to_write = []  # Keep track of (tab_name, dataframe) tuples
    output_file = os.path.join(folder_path, "bacnet-scan.xlsx")
    print(f"\nProcessing {len(files)} files...")

    for file_path in files:
        file_name = os.path.basename(file_path)
        # print(f"\nProcessing file: {file_name}")

        try:
            xls = pd.ExcelFile(file_path)
        except Exception as e:
            print(f"  ❌ Failed to read {file_name}: {e}")
            continue

        # Identify device tabs (anything starting with "device" except "devices")
        device_tabs = [s for s in xls.sheet_names if s.startswith("device") and s != "devices"]

        # Validation: skip files with no device tabs
        if not device_tabs:
            print(f"  ⚠️ Skipping {file_name}: contains only 'devices' tab and no 'device*******' tab.")
            continue

        # Validation: flag files with more than one device tab
        if len(device_tabs) > 1:
            # print(f"  ⚠️ File {file_name} contains multiple device tabs: {device_tabs}")
            pass

        # Step 1: Append 'devices' tab
        if "devices" in xls.sheet_names:
            df_devices = pd.read_excel(xls, sheet_name="devices")
            all_devices_df.append(df_devices)
            # print(f"  ✅ 'devices' tab added with {len(df_devices)} rows")
        else:
            print(f"  ⚠️ No 'devices' tab found in {file_name}")

        # Step 2: Collect device tabs for later writing
        for tab in device_tabs:
            df_tab = pd.read_excel(xls, sheet_name=tab)
            device_tabs_to_write.append((tab, df_tab))
            # print(f"  ✅ '{tab}' tab prepared with {len(df_tab)} rows")

    # Write all tabs to Excel with 'devices' first
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Write combined 'devices' tab first
        if all_devices_df:
            combined_devices = pd.concat(all_devices_df, ignore_index=True)
            combined_devices.to_excel(writer, sheet_name="devices", index=False)
            print(f"\nAll 'devices' tabs combined: {len(combined_devices)} total rows")
        else:
            print("\nNo 'devices' tabs found in any valid files.")

        # Write all device tabs after 'devices'
        for tab_name, df_tab in device_tabs_to_write:
            df_tab.to_excel(writer, sheet_name=tab_name, index=False)

    print(f"\nFinished! Output written to: {output_file}")

def main():
    commands_path = input("Enter the absolute path to the commands Excel file: ").strip()
    folder_path = input("Enter the absolute path to the folder containing device Excel files: ").strip()

    is_valid, bacnet_ids = validate_bacnet_files(commands_path, folder_path)
    if not is_valid:
        print("Validation failed. Fix the missing files before processing.")
        return

    process_bacnet_files(folder_path, bacnet_ids)

if __name__ == "__main__":
    main()