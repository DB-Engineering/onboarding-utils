import subprocess
import re
import os
import sys
import time

def export_building_config(building_code, outfile_path):
    """Run ExportBuildingConfig, poll until result is written to outfile, then clean gibberish."""

    # ----------------------------
    # Parse building code
    # ----------------------------
    try:
        country_code, city_code, building_code_part = building_code.split("-", 2)
    except ValueError:
        print("❌ Invalid building code format. Expected: XX-YYY-ZZZ")
        sys.exit(1)

    country_code = country_code.lower()
    city_code = city_code.lower()
    building_code_part = building_code_part.lower()

    # Ensure parent directory for outfile exists
    os.makedirs(os.path.dirname(outfile_path), exist_ok=True)

    # ----------------------------
    # First command: ExportBuildingConfig
    # ----------------------------
    export_args = [
        "stubby",
        "call",
        "blade:google.cloud.digitalbuildings.v1alpha1.digitalbuildingsservice-prod",
        "google.cloud.digitalbuildings.v1alpha1.DigitalBuildingsService.ExportBuildingConfig",
        "--deadline=60000",
        "--print_status_extensions",
        "--proto2",
        f"name: 'projects/digitalbuildings/countries/{country_code}/cities/{city_code}/buildings/{building_code_part}', profile:'projects/digitalbuildings/profiles/MaintenanceOps'"
    ]

    print("Running export building config command...")
    export_result = subprocess.run(export_args, capture_output=True, text=True)

    if export_result.returncode != 0:
        print("❌ ExportBuildingConfig failed (return code != 0):")
        print(export_result.stderr.strip())
        if export_result.stdout:
            print("Export stdout:\n", export_result.stdout)
        sys.exit(1)

    combined_out = (export_result.stdout or "") + "\n" + (export_result.stderr or "")

    # ----------------------------
    # Extract operation_name
    # ----------------------------
    match = re.search(r"name:\s*['\"]([^'\"]+)['\"]", combined_out)
    if not match:
        print("❌ Failed to extract operation_name from ExportBuildingConfig output")
        sys.exit(1)

    operation_name = match.group(1)

    # ----------------------------
    # Second command: GetOperation
    # ----------------------------
    get_op_args = [
        "stubby",
        "call",
        "blade:google.cloud.digitalbuildings.v1alpha1.digitalbuildingsservice-prod",
        "google.cloud.digitalbuildings.v1alpha1.DigitalBuildingsService.GetOperation",
        "--print_status_extensions",
        "--proto2",
        f"--outfile={outfile_path}",
        "--binary_output",
        f"name: 'projects/digitalbuildings/countries/{country_code}/cities/{city_code}/buildings/{building_code_part}', profile:'projects/digitalbuildings/profiles/MaintenanceOps', operation_name: '{operation_name}'"
    ]

    time.sleep(10)

    for attempt in range(1, 8):  # 7 tries max
        print(f"Checking operation status (attempt {attempt})...")
        get_op_result = subprocess.run(get_op_args, capture_output=True, text=True)

        if get_op_result.returncode != 0:
            print(f"⚠️ GetOperation failed with exit code {get_op_result.returncode}")
            if get_op_result.stderr:
                print("stderr:\n", get_op_result.stderr.strip())

        # Try to read the outfile and check for "running"
        if os.path.exists(outfile_path):
            try:
                with open(outfile_path, "r", encoding="utf-8", errors="ignore") as fh:
                    content = fh.read()
                if content.strip():
                    if "running" in content.lower():
                        print("Operation still running — retrying in 10 seconds...")
                    else:
                        #print(f"✅ Export appears successful. Config written to: {outfile_path}")
                        clean_export_file(outfile_path)
                        return True
            except Exception as e:
                print(f"Warning: couldn't read outfile {outfile_path}: {e}")

        if attempt < 7:
            time.sleep(10)

    print("❌ Export did not complete successfully (still running after 7 attempts).")
    sys.exit(1)


def clean_export_file(outfile_path):
    """Remove gibberish characters before CONFIG_METADATA: in the exported file."""
    try:
        with open(outfile_path, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()

        marker = "CONFIG_METADATA:"
        idx = content.find(marker)
        if idx == -1:
            print("⚠️ Warning: CONFIG_METADATA not found in file. Leaving file unchanged.")
            return

        cleaned_content = content[idx:]
        with open(outfile_path, "w", encoding="utf-8") as fh:
            fh.write(cleaned_content)

        print("✅ Building config successfully refreshed")

    except Exception as e:
        print(f"⚠️ Failed to clean file {outfile_path}: {e}")


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    building_code = input("Enter building code (format XX-YYY-ZZZ): ").strip()
    outfile_path = input("Enter absolute path for output full_building_config.yaml: ").strip()

    export_building_config(building_code, outfile_path)