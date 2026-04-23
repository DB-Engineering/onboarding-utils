from helpers import helpers
from models import cloud_models, dbo_models
import sys

# ----------------------------
# Normalization + diff logic
# ----------------------------

def normalize(value):
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize(v) for v in value]

    if isinstance(value, str):
        v = value.strip()

        if v.isdigit():
            return int(v)

        try:
            return float(v)
        except ValueError:
            return v

    return value


def compute_update_mask(new_entity, existing_entity):
    update_mask = []
    ignore_fields = {"operation", "update_mask"}

    for key in new_entity:
        if key in ignore_fields:
            continue

        new_val = normalize(new_entity.get(key))
        existing_val = normalize(existing_entity.get(key))

        if key not in existing_entity or new_val != existing_val:
            update_mask.append(key)

    return update_mask


# ----------------------------
# Matching logic
# ----------------------------

def find_match(entity_dict, existing_entities):
    def match_by(field):
        return [
            (guid, data)
            for guid, data in existing_entities.items()
            if str(data.get(field)) == str(entity_dict.get(field))
        ]

    # 1. cloud_device_id
    matches = match_by("cloud_device_id")
    if matches:
        if len(matches) > 1:
            print(f"[WARN] Multiple matches on cloud_device_id for {entity_dict.get('display_name')} → skipping")
            return None
        return matches[0]

    # 2. display_name
    matches = match_by("display_name")
    if matches:
        if len(matches) > 1:
            print(f"[WARN] Multiple matches on display_name for {entity_dict.get('display_name')} → skipping")
            return None
        return matches[0]

    # 3. code
    matches = match_by("code")
    if matches:
        if len(matches) > 1:
            print(f"[WARN] Multiple matches on code for {entity_dict.get('code')} → skipping")
            return None
        return matches[0]

    return None


# ----------------------------
# Entity builder
# ----------------------------

def build_entities(loadsheet, site_model, device_discovery):
    assets = loadsheet["assetName"].unique().tolist()
    entities = {}

    for asset in assets:
        asset_loadsheet = loadsheet.loc[
            loadsheet["assetName"] == asset,
            ["controlProgram", "typeName", "assetName", "standardFieldName",
             "units", "deviceId", "objectType", "objectId", "isMissing"]
        ].astype(str)

        display_name = asset
        code = ", ".join(sorted(asset_loadsheet.controlProgram.unique().tolist()))
        namespace = "HVAC"
        type_name = asset_loadsheet.typeName.unique().tolist()[0]

        fields = asset_loadsheet.set_index("standardFieldName", drop=True)[
            ["units", "deviceId", "objectType", "objectId", "isMissing"]
        ].T.to_dict()

        # Reset device per asset
        device = None

        for _, v in fields.items():
            if v.get("isMissing") == "YES":
                continue

            device = site_model.get_device_by_object_id(
                v.get("deviceId"),
                f"{v.get('objectType')}:{v.get('objectId')}"
            )

            if not device:
                continue

            if not device.numeric_id:
                device.numeric_id = device_discovery.loc[
                    device_discovery.device_id == device.proxy_id,
                    'device_num_id'
                ].item()

            if device.proxy_id and device.numeric_id:
                break

        if not device:
            print(f"[ERROR] Device missing from site model: {asset}, {code}")
            continue

        if not (device.proxy_id and device.numeric_id):
            print(f"[ERROR] Device missing from clearblade discovery: {asset}")

        entity = dbo_models.Entity(
            code=code,
            etag="",
            proxy_id=device.proxy_id,
            cloud_device_id=device.numeric_id,
            namespace=namespace,
            type_name=type_name,
            display_name=display_name,
            operation="ADD"
        )

        entity.add_fields_from_dict(fields)

        entity_dict = entity.to_dict()
        entities.update(entity_dict)

    return entities


# ----------------------------
# Main: Generate building config
# ----------------------------

def main():
    loadsheet_path = input("Insert path to loadsheet (.xlsx): ")
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    site_model_path = input("Insert path to site model: ")
    existing_config_path = input("Insert path for existing building config file (.yaml) or press ENTER to skip: ")
    output_path = input("Insert path for output building config file (.yaml): ")

    if not loadsheet_path or not device_discovery_path or not site_model_path or not output_path:
        raise ValueError("Necessary inputs are missing.")

    device_discovery = helpers.load_file(device_discovery_path)
    site_model = cloud_models.SiteModel.from_dir(site_model_path)

    loadsheet = helpers.load_file(loadsheet_path)
    loadsheet = loadsheet.loc[loadsheet["required"] == "YES"]

    # ----------------------------
    # Load existing config (optional)
    # ----------------------------
    if existing_config_path:
        existing_config = helpers.load_file(existing_config_path)
        existing_entities = {
            k: v for k, v in existing_config.items()
            if k != "CONFIG_METADATA"
        }
    else:
        existing_entities = {}

    new_entities = build_entities(loadsheet, site_model, device_discovery)

    building_config = {
        "CONFIG_METADATA": {"operation": "UPDATE"}
    }

    for new_guid, new_entity in new_entities.items():

        match = find_match(new_entity, existing_entities) if existing_entities else None

        if match:
            existing_guid, existing_entity = match

            # Preserve etag
            new_entity["etag"] = existing_entity.get("etag", "")

            # Convert to UPDATE
            new_entity["operation"] = "UPDATE"

            # Compute update mask (ONLY for UPDATE)
            update_mask = compute_update_mask(new_entity, existing_entity)

            if update_mask:
                new_entity["update_mask"] = update_mask

            building_config[existing_guid] = new_entity

        else:
            # ADD path (no update_mask EVER)
            new_entity["operation"] = "ADD"

            print(f"[INFO] No match found → ADD entity: {new_entity.get('display_name')} ({new_entity.get('code')})")

            building_config[new_guid] = new_entity

    helpers.write_yaml(output_path, building_config)

    print(f"Building config successfully generated: {output_path}")


if __name__ == "__main__":
    main()