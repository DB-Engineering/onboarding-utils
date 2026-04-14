import pandas as pd
import os
import json
import yaml
from collections import OrderedDict

OBJECT_ID_MAP_BMS_TO_CAMEL = {
    "AV": "analogValue",
    "AI": "analogInput",
    "AO": "analogOutput",
    "BV": "binaryValue",
    "BI": "binaryInput",
    "BO": "binaryOutput",
    "MSV": "multiStateValue"
}

OBJECT_ID_MAP_SITE_MODEL_TO_BMS = {
    "ANALOG_VALUE": "AV",
    "ANALOG_INPUT": "AI",
    "ANALOG_OUTPUT": "AO",
    "BINARY_VALUE": "BV",
    "BINARY_INPUT": "BI",
    "BINARY_OUTPUT": "BO",
    "MULTI_STATE_VALUE": "MSV"
}

def snake_to_camel(x: str):
    if not isinstance(x, str):
        return None
    x.replace("_", "-")
    parts = x.split('-')
    if len(parts) > 1:
        return parts[0] + ''.join(p.capitalize() for p in parts[1:])
    else: return x

def camel_to_snake(text):
    res = []
    for i, char in enumerate(text):
        # If the char is uppercase and not the very first character,
        # prepend an underscore.
        if char.isupper() and i > 0:
            res.append('_')
        res.append(char.lower())
    return "".join(res)

def device_id_to_ip_addr(device_id: str):
    """
    Takes in a device bacnet id and returns string of format: {network}:{ip} for bacnet-scan/devices "ip_address" column.
    {network} part is first 5 digits, the remaining 2 is ip. If ip starts with "0", "0" is omitted.
    If length of numeric part is not 7 digits long, returns the same string back.
    Ex: 
        bacnet3002617 -> 30026:17
        3002617 -> 30026:17
        3002605 -> 30026:5 (ip part starts with 0)
        300260-> 300260 (not 7 digits)
    """ 
    device_id = "".join(d for d in device_id if d.isdigit())
    if len(device_id)==7:
        network = device_id[:5]
        ip = device_id[:2]
        if ip[0] == 0:
            ip = ip[1:]
        return f"{network}:{ip}"
    else:
        return device_id

def load_file(file_path, **kwargs):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    root, ext = os.path.splitext(file_path)

    try:
        if not ext:
            print(f"Not a file: {file_path}")
            return None
        elif ext == ".csv":
            return pd.read_csv(file_path, dtype=str, **kwargs)
            return None
        elif ext == ".xlsx":
            return pd.read_excel(file_path, dtype=str, **kwargs)
        elif ext == ".json":
            with open(file_path, "r") as f:
                file = json.load(f)
                return file
        elif ext == ".yaml":
            with open(file_path, "r") as f:
                file = yaml.safe_load(f)
            return file
        else:
            print(f"Unknown file format: {file_path}")
            return None
    except Exception as e:
        print(f"Could not load file {file_path}: {e}")

def write_yaml(file_path, data):
    """Write YAML with OrderedDict handling, Boolean -> ON/OFF, and entry spacing."""
    
    def convert(obj):
        # 1. Handle Booleans first to avoid accidental string replaces later
        if isinstance(obj, bool):
            return "ON" if obj else "OFF"
        elif isinstance(obj, (OrderedDict, dict)):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(i) for i in obj]
        else:
            return obj

    # Step 1: Prepare the clean data
    normal_dict = convert(data)

    # Step 2: Write to file using a single pass for spacing
    with open(file_path, "w") as f:
        # We dump one top-level key at a time to naturally inject spacing
        items = list(normal_dict.items())
        for i, (key, value) in enumerate(items):
            # Dump the individual entry
            yaml.safe_dump({key: value}, f, 
                           default_flow_style=False, 
                           sort_keys=False, 
                           allow_unicode=True)
            
            # If it's not the last entry, add a blank line
            if i < len(items) - 1:
                f.write("\n")

def map_units(fieldname):
    if any([
        "alarm" in fieldname,
        "run_command" in fieldname,
        "run_status" in fieldname,
        "damper_command" in fieldname,
        "damper_status" in fieldname,
        "override_status" in fieldname,
        "mode" in fieldname,
        "valve_command" in fieldname,
        "valve_status" in fieldname,
        "count" in fieldname,
        "powerfactor" in fieldname
    ]):
        return "no-units"
    elif "percentage" in fieldname:
        return "percent"
    elif "temperature" in fieldname:
        return "degrees-fahrenheit"
    elif "frequency" in fieldname:
        return "hertz"
    elif "current" in fieldname:
        return "amperes"
    elif "torque" in fieldname:
        return "newton-meters"
    elif "cooling_thermal_power" in fieldname:
        return "btus-per-hour"
    elif "cooling_thermal_power" in fieldname:
        return "tons-of-refrigeration"
    elif "power" in fieldname:
        return "kilowatts"
    elif "illuminance" in fieldname:
        return "lux"
    elif "energy_accumulator" in fieldname:
        return "kilowatt-hours"
    elif "time_accumulator" in fieldname:
        return "hours"
    elif "load_power" in fieldname:
        return "tons-of-refrigeration"
    elif "reactive_power" in fieldname:
        return "kilovolt-amperes-reactive"
    elif "reactive_energy_accumulator" in fieldname:
        return "kilovolt-ampere-hours"
    elif "thermal_energy_accumulator" in fieldname:
        return "tons-of-refrigeration"
    elif "thermalefficiency" in fieldname:
        return "kilowatts-per-ton"
    elif "water_volume_accumulator" in fieldname:
        return "us-gallons"
    elif "heating_thermal_power" in fieldname:
        return "btus-per-hour"
    elif "enthalpy" in fieldname:
        return "btus-per-pound-dry-air"
    elif "humidity" in fieldname:
        return "percent-relative-humidity"
    elif "voltage" in fieldname:
        return "volts"
    elif "air" in fieldname and "pressure" in fieldname:
        return "inches-of-water"
    elif "filter" in fieldname and "pressure" in fieldname:
        return "inches-of-water"
    elif any(["refrigerant" in fieldname, "water" in fieldname, "differential" in fieldname]) and "pressure" in fieldname:
        return "pounds-force-per-square-inch"
    elif "air" in fieldname and "flowrate" in fieldname:
        return "cubic-feet-per-minute"
    elif "water" in fieldname and "flowrate" in fieldname:
        return "us-gallons-per-minute"
    elif fieldname in ["flowrate_sensor", "flowrate_setpoint"]:
        return "us-gallons-per-minute"
    elif "concentration" in fieldname:
        return "parts-per-million"
    else:
        return None

def map_states(field_name):
    if any(["sensor" in field_name, 
            "setpoint" in field_name,
            "percentage" in field_name,
            "accumulator" in field_name,
            "count" in field_name
           ]):
        return None
    if "alarm" in field_name: 
        return {"ACTIVE": "1.0", "INACTIVE": "0.0"}
    if "occupancy_status" in field_name: 
        return {"OCCUPIED": "1.0", "UNOCCUPIED": "0.0"}
    if "user_occupancy_override_status" in field_name: 
        return {"ENABLED": "1.0", "DISABLED": "0.0"}
    if any(["run_command" in field_name, "run_status" in field_name]): 
        return {"ON": "1.0", "OFF": "0.0"}
    if any(["damper_command" in field_name, "damper_status" in field_name]): 
        return {"OPEN": "1.0", "CLOSED": "0.0"}
    if any(["valve_command" in field_name, "valve_status" in field_name]):  
        return {"OPEN": "1.0", "CLOSED": "0.0"}
    if all(["economizer" in field_name, "mode" in field_name]):  
        return {"ON": "1.0", "OFF": "0.0"}
    else: 
        return None