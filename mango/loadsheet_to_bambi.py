import pandas as pd
import re
import os
import warnings  # <--- Add this
from collections import defaultdict, Counter

# ----------------------------
# CONFIG
# ----------------------------

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


def build_lookup(df, col="device_id"):
    if col not in df.columns:
        return set()

    return set(df[col].dropna().astype(str))


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


def build_ref(device_raw_id, obj_type, object_id):
    if not obj_type or not object_id:
        return None

    device_numeric = str(device_raw_id).replace("DEV:", "").strip()

    return f"DP_{device_numeric}_{obj_type}_{object_id}"


# ----------------------------
# MAIN PIPELINE
# ----------------------------

def process_excel(bambi_file, loadsheet_file, mapping_file):

    xls = pd.ExcelFile(bambi_file)
    sheets = {name: xls.parse(name) for name in xls.sheet_names}

    loadsheet = pd.read_excel(loadsheet_file)

    proxy_map = load_proxy_map(mapping_file)

    # ----------------------------
    # DEVICE LOOKUPS
    # ----------------------------

    device_sets = {}

    for tab in TABS_TO_VALIDATE:
        device_sets[tab] = build_lookup(
            sheets.get(tab, pd.DataFrame())
        )

    # ----------------------------
    # GROUP LOADSHEET BY DEVICE
    # ----------------------------

    grouped = defaultdict(list)

    for _, row in loadsheet.iterrows():

        asset = normalize_device_key(row.get("assetName"))

        try:
            device_id = get_proxy_id(
                row.get("assetName"),
                proxy_map
            )

        except Exception as e:
            print(f"Skipping grouping row due to error: {e}")
            continue

        if not asset or not device_id:
            continue

        grouped[device_id].append(row)

    # ----------------------------
    # VALIDATION
    # ----------------------------

    print("\nDEVICE VALIDATION")
    print("------------------")

    summary = Counter()

    for device_id in sorted(grouped.keys()):

        missing = []

        for tab in TABS_TO_VALIDATE:

            if device_id not in device_sets[tab]:
                missing.append(tab)

        if missing:

            print(f"{device_id}: missing in {', '.join(missing)}")

            for m in missing:
                summary[m] += 1

        else:

            print(f"{device_id}: OK")
            summary["OK"] += 1

    # ----------------------------
    # POINT BUILDING
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

        obj_type = map_object_type(object_type_raw)

        if not obj_type:
            continue

        template = f"{device_id}_template"

        point_name = standard_field

        ref = build_ref(
            device_raw_id,
            obj_type,
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
            "writable": False,
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

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        for name, df in sheets.items():

            if name == TAB_POINTS:
                updated_points.to_excel(
                    writer,
                    sheet_name=name,
                    index=False
                )

            else:
                df.to_excel(
                    writer,
                    sheet_name=name,
                    index=False
                )

    # ----------------------------
    # SUMMARY
    # ----------------------------

    print("\nVALIDATION SUMMARY")
    print("------------------")

    print(f"OK: {summary['OK']}")
    print(f"Missing system: {summary[TAB_SYSTEM]}")
    print(f"Missing cloud: {summary[TAB_CLOUD]}")
    print(f"Missing gateway: {summary[TAB_GATEWAY]}")
    print(f"Missing localnet: {summary[TAB_LOCALNET]}")
    print(f"Missing pointset: {summary[TAB_POINTSET]}")

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