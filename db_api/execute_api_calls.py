import subprocess
import re
import os
import time
import sys
from . import update_etags
from . import export_building_config

# ----------------------------
# Helper functions
# ----------------------------
def run_onboard_and_get_status(building_code, topology_file_path, result_file_path):
    try:
        _, city_code, building_code_part = building_code.split("-", 2)
    except ValueError:
        print("Invalid building code format. Expected: US-XXX-YYY")
        print("\a")  # Chime for failure
        return False

    city_code = city_code.lower()
    building_code_part = building_code_part.lower()

    os.makedirs(os.path.dirname(result_file_path), exist_ok=True)

    onboard_args = [
        "stubby",
        "call",
        "blade:google.cloud.digitalbuildings.v1alpha1.digitalbuildingsservice-prod",
        "google.cloud.digitalbuildings.v1alpha1.DigitalBuildingsService.OnboardBuilding",
        "--print_status_extensions",
        "--proto2",
        f"name: 'projects/digitalbuildings/countries/us/cities/{city_code}/buildings/{building_code_part}', profile:'projects/digitalbuildings/profiles/MaintenanceOps'",
        "--set_field",
        f"topology_file=readfile({topology_file_path})"
    ]
    print("Running onboarding command...")
    onboard_result = subprocess.run(onboard_args, capture_output=True, text=True)
    if onboard_result.returncode != 0:
        print("OnboardBuilding failed (return code != 0):")
        print(onboard_result.stderr.strip())
        if onboard_result.stdout:
            print("Onboard stdout:\n", onboard_result.stdout)
        print("\a")
        return False

    onboard_combined = (onboard_result.stdout or "") + "\n" + (onboard_result.stderr or "")
    match = re.search(r'name:\s*["\']([^"\']+)["\']', onboard_combined)
    if not match:
        print("Failed to extract operation name from OnboardBuilding output")
        print("\a")
        return False

    operation_name = match.group(1)

    get_op_args = [
        "stubby",
        "call",
        "blade:google.cloud.digitalbuildings.v1alpha1.digitalbuildingsservice-prod",
        "google.cloud.digitalbuildings.v1alpha1.DigitalBuildingsService.GetOperation",
        "--print_status_extensions",
        "--proto2",
        f"--outfile={result_file_path}",
        "--binary_output",
        f"name: 'projects/digitalbuildings/countries/us/cities/{city_code}/buildings/{building_code_part}', profile:'projects/digitalbuildings/profiles/MaintenanceOps', operation_name: '{operation_name}'"
    ]

    time.sleep(10)
    check_count = 1
    while True:
        print(f"Checking operation status (attempt {check_count})...")
        get_op_result = subprocess.run(get_op_args, capture_output=True, text=True)

        file_content = ""
        try:
            if os.path.exists(result_file_path):
                with open(result_file_path, "r", encoding="utf-8", errors="ignore") as fh:
                    file_content = fh.read()
        except Exception as e:
            print(f"Warning: couldn't read {result_file_path}: {e}")

        combined_out = file_content.strip() or ((get_op_result.stdout or "") + "\n" + (get_op_result.stderr or ""))
        combined_out = combined_out.strip()

        if get_op_result.returncode != 0:
            print("Warning: GetOperation returned non-zero exit code:", get_op_result.returncode)
            if get_op_result.stderr:
                print("GetOperation stderr:\n", get_op_result.stderr.strip())

        if re.search(r"\brunning\b", combined_out, re.I):
            if check_count <= 3:
                wait_time = 10
            elif check_count <= 6:
                wait_time = 30
            else:
                wait_time = 60
            print(f"Operation still running — will retry in {wait_time} seconds")
            time.sleep(wait_time)
            check_count += 1
            continue

        if "Successfully completed onboard operation." not in combined_out:
            print(f"Config onboarding failed.")
            print("\a")
            return False
        else:
            print(f"Config onboarding succeeded.")
        return True

def build_result_path(cfg_path):
    parent_dir = os.path.dirname(cfg_path)
    base_dir = os.path.basename(parent_dir)
    root_dir = os.path.dirname(parent_dir)
    result_subdir = base_dir.replace("_entities", "_results")
    result_dir = os.path.join(root_dir, "results", result_subdir)
    os.makedirs(result_dir, exist_ok=True)
    base, ext = os.path.splitext(os.path.basename(cfg_path))
    return os.path.join(result_dir, f"{base}_result{ext}")


def analyze_results(result_files):
    success_count = 0
    fail_count = 0
    failed_files = []

    for res_file, orig_cfg, was_skipped in result_files:
        if was_skipped:
            success_count += 1
            continue
        content = ""
        try:
            with open(res_file, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
        except Exception as e:
            print(f"Warning: couldn't read {res_file}: {e}")

        if "Successfully completed onboard operation." in content:
            success_count += 1
        else:
            fail_count += 1
            failed_files.append(os.path.basename(orig_cfg))

    print("\n===== SUMMARY =====")
    print(f"✅ Successful onboardings: {success_count}")
    print(f"❌ Failed onboardings: {fail_count}")
    if failed_files:
        print("\nFailed config files:")
        for f in failed_files:
            print(f"  - {f}")
    # Final chime for script completion
    for _ in range(3):
        print("\a", end="", flush=True)
        time.sleep(0.5)


# ----------------------------
# Main script
# ----------------------------
def main():
    building_code = input("Enter building code (format US-XXX-YYY): ").strip()

    print("\nChoose input mode:")
    print("1) Manually enter config file paths")
    print("2) Enter a directory containing config files\n")
    mode = input("Enter 1 or 2: ").strip()

    config_files = []
    results_dir_option1 = None  # New variable for manual-entry results directory

    if mode == "1":
        print("\nEnter the absolute paths to your config files, one per line.")
        print("Type 'd' when you are done.\n")
        while True:
            path = input("Config file path: ").strip()
            if path.lower() == "d":
                break
            if not os.path.isfile(path):
                print(f"❌ File not found: {path}")
                continue
            config_files.append(path)

        if config_files:
            # Prompt user once for results directory
            results_dir_option1 = input("\nEnter the absolute path to the directory where result files should be saved: ").strip()
            if not results_dir_option1:
                print("No directory provided. Exiting.")
                sys.exit(1)
            os.makedirs(results_dir_option1, exist_ok=True)

    elif mode == "2":
        dir_path = input("\nEnter the absolute path to the config directory: ").strip()
        if not os.path.isdir(dir_path):
            print(f"❌ Directory not found: {dir_path}")
            sys.exit(1)
        config_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.endswith(".yaml")]
        config_files.sort()
        if not config_files:
            print(f"No .yaml files found in {dir_path}")
            sys.exit(0)
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)

    if not config_files:
        print("No config files provided. Exiting.")
        sys.exit(0)

    building_config_path = input("\nEnter absolute path to full_building_config.yaml: ").strip()

    export_building_config_prompt = input("\nWould you like to export a new building config? Y/N: " )
    if export_building_config_prompt.lower() == 'n':
        if not os.path.isfile(building_config_path):
            print(f"ERROR: File not found: {building_config_path}")
            sys.exit(1)
    if export_building_config_prompt.lower() == 'y':
        print("\n=== Exporting new building config ===")
        try:
            export_building_config.export_building_config(building_code, building_config_path)
        except SystemExit:
            print("⚠️ Warning: Failed to update building config. Continuing with existing file...")

    # ----------------------------------
    # Onboard each entity config
    # ----------------------------------
    result_files = []
    for cfg in config_files:
        if mode == "1":
            # For manual entry, use user-specified results directory
            base, ext = os.path.splitext(os.path.basename(cfg))
            result_file = os.path.join(results_dir_option1, f"{base}_result{ext}")
        else:
            # For directory mode, use existing build_result_path
            result_file = build_result_path(cfg)

        skip_file = False
        if os.path.exists(result_file):
            try:
                with open(result_file, "r", encoding="utf-8", errors="ignore") as fh:
                    content = fh.read()
                if "Successfully completed onboard operation." in content:
                    print(f"✅ Skipping {cfg} — already successfully onboarded.")
                    skip_file = True
            except Exception as e:
                print(f"Warning: couldn't read {result_file}: {e}")

        if skip_file:
            result_files.append((result_file, cfg, True))
            continue

        print(f"\n--- Processing config file: {cfg} ---")
        transfer_etags.sync_etags(building_config_path, cfg)
        success = run_onboard_and_get_status(building_code, cfg, result_file)
        result_files.append((result_file, cfg, False))
        print(f"Moving to next file...")

    analyze_results(result_files)

if __name__ == "__main__":
    main()