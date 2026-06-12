import pandas as pd
import re
import os
import warnings  # <--- Add this
from collections import defaultdict, Counter

from models import bambi_models
from helpers import helpers

# ----------------------------
# CONFIG
# ----------------------------
REQUIRED_COLS = ["location", "controlProgram", "name", "type", "deviceId", "objectType", "objectId", "objectName", "path", "required", 
                 "units", "manuallyMapped", "isMissing", "building", "generalType", "typeName", "assetName", "standardFieldName"]

TAB_SYSTEM = "system"
TAB_CLOUD = "cloud"
TAB_GATEWAY = "gateway"
TAB_LOCALNET = "localnet"
TAB_POINTSET = "pointset"
TAB_POINTS = "points"

OBJECT_TYPE_MAP = {
    "BV": "BINARY_VALUE",
    "BI": "BINARY_INPUT",
    "BO": "BINARY_OUTPUT",
    "AV": "ANALOG_VALUE",
    "AI": "ANALOG_INPUT",
    "AO": "ANALOG_OUTPUT",
}

TABS_TO_VALIDATE = [
    TAB_SYSTEM,
    TAB_CLOUD,
    TAB_GATEWAY,
    TAB_LOCALNET,
    TAB_POINTSET,
]

def main():
    loadsheet_path = input(
        "Enter absolute path to loadsheet (.xlsx): "
    ).strip().strip('"')

    bambi_path = input(
        "Enter absolute path to BAMBI sheet (.xlsx): "
    ).strip().strip('"')

    mapping_path = input(
        "Enter absolute path to assetName <-> proxyID map (.xlsx): "
    ).strip().strip('"')

    if not os.path.exists(loadsheet_path):
        raise FileNotFoundError(
            f"Loadsheet not found: {loadsheet_path}"
        )

    if not os.path.exists(bambi_path):
        raise FileNotFoundError(
            f"BAMBI not found: {bambi_path}"
        )

    if not os.path.exists(mapping_path):
        raise FileNotFoundError(
            f"Proxy mapping file not found: {mapping_path}"
        )

    process_excel(
        bambi_path,
        loadsheet_path,
        mapping_path
    )

# ----------------------------
# PROXY MAP LOADER
# ----------------------------

def load_proxy_map(mapping_path):
    df = pd.read_excel(mapping_path)

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]

    if "asset_name" not in df.columns or "proxy_id" not in df.columns:
        raise ValueError(
            "Mapping file must contain 'asset_name' and 'proxy_id' columns"
        )

    return dict(zip(
        df["asset_name"].astype(str).str.strip(),
        df["proxy_id"].astype(str).str.strip()
    ))


def get_proxy_id(asset_name, proxy_map):
    if pd.isna(asset_name):
        return None

    asset_name = str(asset_name).strip()

    if asset_name not in proxy_map:
        raise ValueError(f"Missing proxy_id mapping for assetName: {asset_name}")

    return proxy_map[asset_name]


# ----------------------------
# HELPERS
# ----------------------------

def normalize_device_key(asset_name: str):
    return str(asset_name).strip().upper() if isinstance(asset_name, str) else None


def map_object_type(obj_type):
    if pd.isna(obj_type):
        return None

    return OBJECT_TYPE_MAP.get(str(obj_type).strip().upper())


def clean_object_id(value):
    if pd.isna(value):
        return None

    try:
        if isinstance(value, float) and value.is_integer():
            return str(int(value))

        if isinstance(value, int):
            return str(value)

        if isinstance(value, str):
            return str(int(float(value)))

    except:
        return None

    return str(value)


def build_ref(device_raw_id, obj_type_raw, object_id, new_driver=False):
    if not obj_type_raw or not object_id:
        return None

    device_numeric = str(device_raw_id).replace("DEV:", "").strip()

    if new_driver==True:
        return f"bacnet://{device_numeric}/{obj_type_raw}/{object_id}"
    else:
        obj_type = map_object_type(obj_type_raw)
        return f"DP_{device_numeric}_{obj_type}_{object_id}"


# ----------------------------
# MAIN PIPELINE
# ----------------------------

def process_excel(bambi_file, loadsheet_file, mapping_file):

    xls = pd.ExcelFile(bambi_file)
    sheets = {name: xls.parse(name) for name in xls.sheet_names}

    loadsheet = pd.read_excel(loadsheet_file)
    loadsheet = loadsheet.loc[(loadsheet['required']=="YES") & (loadsheet['isMissing']!="YES"), REQUIRED_COLS]
    loadsheet["objectId"] = loadsheet.loc[loadsheet["objectId"].isna()==False, "objectId"].astype(int).astype(str)

    proxy_map = load_proxy_map(mapping_file)

    prompt_driver_version = input("Are you populating BAMBI sheet for the driver version >=5.3.1? Y/N: ")

    site_name = loadsheet.at[0, "building"]

    # ----------------------------
    # BUILD DEVICE LOOKUP
    # ----------------------------

    device_lookup = {}
    assets = loadsheet["assetName"].unique().tolist()

    for asset in assets:
        asset_loadsheet = loadsheet.loc[loadsheet["assetName"]==asset, ["controlProgram", "typeName", "assetName", "standardFieldName", 
                                                                        "units", "deviceId", "objectType", "objectId", "isMissing"]]
        try:
            device_id = get_proxy_id(
                asset,
                proxy_map
            )

        except Exception as e:
            print(f"Skipping grouping row due to error: {e}")
            continue

        code = ", ".join(sorted(asset_loadsheet.controlProgram.dropna().unique().tolist()))

        bacnet_ids = sorted([helpers.strip_bacnet_id(i) for i in asset_loadsheet.deviceId.dropna().unique().tolist()])
        networks = sorted([i[:5] for i in bacnet_ids])

        fields = asset_loadsheet.set_index("standardFieldName", drop=True)[["units", "deviceId", "objectType", 
                                                                            "objectId", "isMissing"]].T.to_dict()

        device = bambi_models.BAMBIDevice(
            proxy_id=device_id,
            system_name=code,
            bacnet_ids=bacnet_ids,
            networks=networks
            )
        device.add_points_from_dict(fields)

        device_lookup[device_id] = device

    # ----------------------------
    # BUILDING SYSTEM, CLOUD, GATEWAY, LOCALNET, POINTSET TABS
    # ----------------------------
    updated_system = sheets.get(TAB_SYSTEM, pd.DataFrame()).copy()
    updated_cloud = sheets.get(TAB_CLOUD, pd.DataFrame()).copy()
    updated_gateway = sheets.get(TAB_GATEWAY, pd.DataFrame()).copy()
    updated_localnet = sheets.get(TAB_LOCALNET, pd.DataFrame()).copy()
    updated_pointset = sheets.get(TAB_POINTSET, pd.DataFrame()).copy()

    for device_id, device_obj in sorted(device_lookup.items()):
        if device_id not in updated_system['device_id'].to_list():
            new_row = pd.DataFrame([{"device_id": device_id,
                                     "name": device_obj.system_name,
                                     "description": ", ".join([f"bacnet-{i}" for i in device_obj.bacnet_ids]),
                                     "location.site": site_name
                                     }])
            updated_system = pd.concat([updated_system, new_row], ignore_index=True)

        if device_id not in updated_cloud['device_id'].to_list():
            new_row = pd.DataFrame([{"device_id": device_id,
                                     "resource_type": "PROXIED"
                                     }])
            updated_cloud = pd.concat([updated_cloud, new_row], ignore_index=True)

        if device_id not in updated_gateway['device_id'].to_list():
            new_row = pd.DataFrame([{"device_id": device_id,
                                     "gateway_id": "CGW-1",
                                     "target.family": "vendor"
                                     }])
            updated_gateway = pd.concat([updated_gateway, new_row], ignore_index=True)

        if device_id not in updated_localnet['device_id'].to_list():
            new_row = pd.DataFrame([{"device_id": device_id,
                                     "parent.target": "CGW-1",
                                     "parent.family": "bacnet",
                                     "families.bacnet.addr": ", ".join(device_obj.bacnet_ids),
                                     "families.bacnet.network": ", ".join(device_obj.networks),
                                     "families.iot.addr": device_id
                                     }])
            updated_localnet = pd.concat([updated_localnet, new_row], ignore_index=True)

        if device_id not in updated_pointset['device_id'].to_list():
            new_row = pd.DataFrame([{"device_id": device_id,
                                     "points_template_name": f"{device_id}_template"
                                     }])
            updated_pointset = pd.concat([updated_pointset, new_row], ignore_index=True)

    # Add information to gateway device
    # Assuming that bacnet gateway already exists in initial site model and that its name is CGW-1
    if "CGW-1" in updated_system["device_id"].to_list():
        updated_system.loc[updated_system["device_id"]=="CGW-1", ["description", "node_type", "location.site"]] = ["bacnet communication gateway", 
                                                                                                                   "virtual_device", 
                                                                                                                   site_name]

    if "CGW-1" in updated_cloud["device_id"].to_list():
        updated_cloud.loc[updated_cloud["device_id"]=="CGW-1", "resource_type"] = "GATEWAY"

    if "CGW-1" in updated_gateway["device_id"].to_list():
        updated_gateway["proxy_ids"] = updated_gateway["proxy_ids"].fillna("").astype(str)
        updated_gateway.loc[updated_gateway["device_id"]=="CGW-1", ["proxy_ids", "target.family"]] = [", ".join(sorted(device_lookup.keys())), 
                                                                                                      "vendor"]

    # ----------------------------
    # BUILDING POINTS TAB
    # ----------------------------

    points_df = sheets.get(TAB_POINTS, pd.DataFrame())

    existing_keys = set()
    existing_refs = set()

    if not points_df.empty:

        for _, r in points_df.iterrows():

            existing_keys.add((
                r.get("points_template_name"),
                r.get("point_name")
            ))

            existing_refs.add(r.get("ref"))

    new_points = []

    updated_devices = set()

    written_points = 0
    dup_keys = 0
    dup_refs = 0

    for _, row in loadsheet.iterrows():

        asset = normalize_device_key(row.get("assetName"))

        try:
            device_id = get_proxy_id(
                row.get("assetName"),
                proxy_map
            )

        except Exception as e:
            print(f"Skipping point row due to error: {e}")
            continue

        if not asset or not device_id:
            continue

        standard_field = row.get("standardFieldName")
        object_type_raw = row.get("objectType")
        object_id = clean_object_id(row.get("objectId"))
        units = row.get("units")
        device_raw_id = str(row.get("deviceId", ""))

        if pd.isna(standard_field) or pd.isna(object_type_raw):
            continue

        if not object_type_raw:
            continue

        template = f"{device_id}_template"

        point_name = standard_field

        if prompt_driver_version.lower()=="y":
            ref = build_ref(
                device_raw_id,
                object_type_raw,
                object_id,
                new_driver=True
            )
        else:
            ref = build_ref(
                device_raw_id,
                object_type_raw,
                object_id
            )

        if not ref:
            continue

        key = (template, point_name)

        if key in existing_keys:
            dup_keys += 1

        if ref in existing_refs:
            dup_refs += 1

        existing_keys.add(key)
        existing_refs.add(ref)

        new_points.append({
            "points_template_name": template,
            "point_name": point_name,
            "units": units.replace("-", "_") if isinstance(units, str) else units,
            "type": None,
            "description": None,
            "writable": "false",
            "baseline_value": None,
            "baseline_tolerance": None,
            "baseline_state": None,
            "range_min": None,
            "range_max": None,
            "unchanged_limit_sec": None,
            "cov_increment": None,
            "ref": ref,
            "tags": None,
            "link": None
        })

        written_points += 1

        updated_devices.add(device_id)

    new_points_df = pd.DataFrame(new_points)

    # Suppress the all-NA concatenation FutureWarning since we are just writing to Excel
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        
        updated_points = pd.concat(
            [points_df, new_points_df],
            ignore_index=True
        )

    # ----------------------------
    # FILTER TABS TO UPDATED DEVICES
    # ----------------------------

    def filter_by_devices(df, device_col="device_id"):

        if df.empty or device_col not in df.columns:
            return df.iloc[0:0]

        return df[df[device_col].isin(updated_devices)]

    # Apply filtering

    for tab in TABS_TO_VALIDATE:

        sheets[tab] = filter_by_devices(
            sheets.get(tab, pd.DataFrame())
        )

    # Special handling for points tab

    if not updated_points.empty:

        updated_points = updated_points[
            updated_points["points_template_name"]
            .str.replace("_template", "")
            .isin(updated_devices)
        ]

    # ----------------------------
    # WRITE OUTPUT
    # ----------------------------

    base, ext = os.path.splitext(bambi_file)

    output_file = f"{base}_populated{ext}"

    updated_sheets_map = {
            TAB_SYSTEM: updated_system,
            TAB_CLOUD: updated_cloud,
            TAB_GATEWAY: updated_gateway,
            TAB_LOCALNET: updated_localnet,
            TAB_POINTSET: updated_pointset,
            TAB_POINTS: updated_points,
        }

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        for name, original_df in sheets.items():
                    df_to_write = updated_sheets_map.get(name, original_df)
                    
                    df_to_write.to_excel(
                        writer,
                        sheet_name=name,
                        index=False
                    )

    # ----------------------------
    # SUMMARY
    # ----------------------------

    print("\nPOINT SUMMARY")
    print("-------------")

    print(f"Total points added: {written_points}")
    print(f"Duplicate (template+point): {dup_keys}")
    print(f"Duplicate refs: {dup_refs}")
    print(f"Devices updated: {len(updated_devices)}")

    print(f"\nSaved output: {output_file}")


# ----------------------------
# ENTRY
# ----------------------------

if __name__ == "__main__":
    main()