import yaml
import os
import sys
import re
from collections import OrderedDict

# ----------------------------
# Constants
# ----------------------------
STATE_TO_ACTIVE_INACTIVE = {
    "ON": "1.0",
    "OFF": "0.0",
    "OPEN": "1.0",
    "CLOSED": "0.0",
    "OCCUPIED": "1.0",
    "UNOCCUPIED": "0.0",
    "ACTIVE": "1.0",
    "INACTIVE": "0.0",
    "ENABLED": "1.0",
    "DISABLED": "0.0",
    "PRESENT": "1.0",
    "ABSENT": "0.0",
    "NORMAL": "0.0",
    "REVERSED": "1.0",
    "BYPASS": "1.0"
}

SUBSTRING_TO_STATES = {
    "closed_command": ["ON","OFF"],
    "damper_command": ["OPEN","CLOSED"],
    "disable_command": ["ON","OFF"],
    "down_command": ["ON","OFF"],
    "isolation_command": ["ON","OFF"],
    "open_command": ["ON","OFF"],
    "purge_command": ["ON","OFF"],
    "run_command": ["ON","OFF"],
    "scene_command": ["ACTIVE","INACTIVE"],
    "speed_command": ["ON","OFF"],
    "stop_command": ["ON","OFF"],
    "switch_command": ["ON","OFF"],
    "up_command": ["ON","OFF"],
    "valve_command": ["OPEN","CLOSED"],
    "cooling_mode": ["ON","OFF"],
    "down_mode": ["ENABLED","DISABLED"],
    "economizer_mode": ["ON","OFF"],
    "exercise_mode": ["ON","OFF"],
    "bypass_status": ["BYPASS","INACTIVE"],
    "closed_status": ["OPEN","CLOSED"],
    "communication_status": ["ON","OFF"],
    "damper_status": ["OPEN","CLOSED"],
    "detection_status": ["ACTIVE","INACTIVE"],
    "motion_status": ["PRESENT","ABSENT"],
    "occupancy_status": ["OCCUPIED","UNOCCUPIED"],
    "open_status": ["OPEN","CLOSED"],
    "override_status": ["DISABLED","ENABLED"],
    "position_status": ["OPEN","CLOSED"],
    "power_status": ["ON","OFF"],
    "release_status": ["ACTIVE","INACTIVE"],
    "run_status": ["ON","OFF"],
    "scene_status": ["ACTIVE","INACTIVE"],
    "speed_status": ["ON","OFF"],
    "switch_status": ["ON","OFF"],
    "test_status": ["ACTIVE","INACTIVE"],
    "valve_status": ["OPEN","CLOSED"],
}

# ----------------------------
# YAML helpers
# ----------------------------
def write_yaml(file_path, data):
    """Write YAML with OrderedDict handling, true/false -> ON/OFF, and spacing."""
    def convert(obj):
        if isinstance(obj, OrderedDict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        else:
            return obj

    normal_dict = convert(data)
    with open(file_path, "w") as f:
        yaml.safe_dump(normal_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Post-process: replace true/false with ON/OFF and add blank lines between top-level entries
    with open(file_path, "r") as f:
        lines = f.readlines()

    new_lines = []
    for i, line in enumerate(lines):
        line = line.replace("true", "ON").replace("false", "OFF")
        new_lines.append(line)
        if i + 1 < len(lines):
            next_line = lines[i + 1]
            if line.strip() and not line.startswith(" ") and next_line.strip() and not next_line.startswith(" "):
                new_lines.append("\n")

    with open(file_path, "w") as f:
        f.writelines(new_lines)

# ----------------------------
# Split functions
# ----------------------------
def split_config_files(entity_dict, output_folder, building_guid, building_content):
    os.makedirs(output_folder, exist_ok=True)
    file_counter = 1
    for guid, content in entity_dict.items():
        if guid in ("CONFIG_METADATA", building_guid):
            continue

        new_dict = OrderedDict()
        new_dict["CONFIG_METADATA"] = {"operation": "UPDATE"}
        new_dict[building_guid] = building_content
        new_dict[guid] = content

        out_name = f"update_reporting_config_pt{file_counter}.yaml"
        out_path = os.path.join(output_folder, out_name)
        write_yaml(out_path, new_dict)
        file_counter += 1

# ----------------------------
# Main processing function
# ----------------------------
def process_file(input_file, full_building_config_file):
    with open(input_file, "r") as f:
        mango_config = yaml.safe_load(f)

    with open(full_building_config_file, "r") as f:
        full_building_config = yaml.safe_load(f)

    building_guid, building_content = None, None

    # Find building GUID
    for guid, content in full_building_config.items():
        if isinstance(content, dict) and content.get("type") == "FACILITIES/BUILDING":
            building_guid, building_content = guid, content
            continue
    if not building_guid:
        print("ERROR: No FACILITIES/BUILDING GUID found in full_building_config.yaml")
        sys.exit(1)

    # Create output dict
    update_reporting_entities = OrderedDict()

    # Config processing
    for guid, content in mango_config.items():
        update_reporting_entities[guid] = content
        if update_reporting_entities[guid].get("translation"):
            for field, subfields in update_reporting_entities[guid].get("translation").items():
                # Fix alarms
                if 'alarm' in field:
                    update_reporting_entities[guid]['translation'][field].pop('units')
                    update_reporting_entities[guid]['translation'][field]['states'] = {'ACTIVE' : '1.0'}
                    update_reporting_entities[guid]['translation'][field]['states']['INACTIVE'] = '0.0'
                    continue
                # Fix states
                states_substring = "_".join(re.sub(r"_\d+$", "", field).split("_")[-2:])
                if states_substring in SUBSTRING_TO_STATES:
                    new_states_field = SUBSTRING_TO_STATES[states_substring]
                    update_reporting_entities[guid]['translation'][field].pop('units')
                    update_reporting_entities[guid]['translation'][field]['states'] = {new_states_field[0] : STATE_TO_ACTIVE_INACTIVE[new_states_field[0]]}
                    update_reporting_entities[guid]['translation'][field]['states'][new_states_field[1]] = STATE_TO_ACTIVE_INACTIVE[new_states_field[1]]
                # Correct underscores/hyphens in units
                else:
                    unit_pair = next(iter(update_reporting_entities[guid]['translation'][field]['units']['values'].items()))
                    update_reporting_entities[guid]['translation'][field]['units']['values'][unit_pair[0]] = unit_pair[0].replace("_", "-")
            # Remove duplicate proxy ID
            update_reporting_entities[guid]['code'] = update_reporting_entities[guid]['code'].split(" ", 1)[1]

    # Split/write YAML
    base_dir = os.path.dirname(input_file)
    category_folder = os.path.join(base_dir, 'update_reporting_entities')
    split_config_files(update_reporting_entities, category_folder, building_guid, building_content)

    print("Processing and splitting complete.")
    print("Split files written to subfolders in:", base_dir)

# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    input_file = input("Enter the absolute path to the BAMBI output file: ").strip()
    full_building_config_file = input("Enter the absolute path to full_building_config.yaml: ").strip()
    # input_file = '/usr/local/google/home/aadalen/Documents/Onboarding/mango_onboarding/US-PAO-EM25/configs/mango_export.yaml'
    # full_building_config_file = '/usr/local/google/home/aadalen/Documents/Onboarding/mango_onboarding/US-PAO-EM25/configs/full_building_config.yaml'

    if not os.path.isfile(input_file):
        print(f"ERROR: Mango file not found: {input_file}")
        sys.exit(1)
    if not os.path.isfile(full_building_config_file):
        print(f"ERROR: full_building_config.yaml not found: {full_building_config_file}")
        sys.exit(1)

    process_file(input_file, full_building_config_file)