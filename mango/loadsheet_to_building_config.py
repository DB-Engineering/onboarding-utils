from helpers import helpers
from models import cloud_models, dbo_models
import sys
import yaml

def main():
    loadsheet_path = input("Insert path to loadsheet (.xlsx): ")
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    existing_building_config_path = input("Insert path for existing building config file (.yaml):")
    site_model_path = input("Insert path to site model: ")
    output_path = input("Insert path for output building config file (.yaml): ")

    if any([not loadsheet_path,
            not device_discovery_path,
            not site_model_path,
            not existing_building_config_path,
            not output_path]):
        raise ValueError("[ERROR] Necessary inputs are missing.")

    device_discovery = helpers.load_file(device_discovery_path)
    site_model = cloud_models.SiteModel.from_dir(site_model_path)
    existing_building_config = dbo_models.Site.from_config(existing_building_config_path)
    new_building_config = dbo_models.Site(
        code=existing_building_config.code,
        guid=existing_building_config.guid,
        type=existing_building_config.type,
        etag=existing_building_config.etag
        )

    loadsheet = helpers.load_file(loadsheet_path)
    loadsheet = loadsheet.loc[loadsheet["required"]=="YES"]

    assets = loadsheet["assetName"].unique().tolist()

    for asset in assets:
        device = None
        asset_loadsheet = loadsheet.loc[loadsheet["assetName"]==asset, ["controlProgram", "typeName", "assetName", "standardFieldName", 
                                                                        "units", "deviceId", "objectType", "objectId", "isMissing"]]

        display_name = asset
        code = ", ".join(sorted(asset_loadsheet.controlProgram.dropna().unique().tolist()))
        type = asset_loadsheet.typeName.dropna().unique().tolist()
        if len(type) != 1:
            print(f"[ERROR] Asset has no typeName: {asset}. Check Loadsheet.")
            break
        else:
            type = f"HVAC/{type[0]}"

        asset_loadsheet.set_index("standardFieldName", drop=True)[["deviceId", "objectType", "objectId", "isMissing"]].T.to_dict()

        fields = asset_loadsheet.set_index("standardFieldName", drop=True)[["units", "deviceId", "objectType", 
                                                                            "objectId", "isMissing"]].T.to_dict()

        # get proxy_id and cloud_device_id by objectId
        for k, v in fields.items():
            if v.get("isMissing") == "YES": continue

            object_type = v.get('objectType')
            object_id = v.get('objectId')

            if not object_type and not object_id: continue
            
            device = site_model.get_device_by_object_id(
                                    v.get("deviceId"), 
                                    f"{object_type}:{str(object_id)}"
                                    )

            if not device: continue

            elif not device.numeric_id:
                device_match = device_discovery.loc[device_discovery.device_id==device.proxy_id, 'device_num_id']
                if device_match.empty:
                    print(f"[WARNING] No numeric_id found for proxy_id: {device.proxy_id}")
                    device.numeric_id = None
                elif len(device_match) > 1:
                    print(f"[WARNING] Multiple IDs found for {device.proxy_id}, {', '.join(device_match.tolist())}. Using the first one.")
                    device.numeric_id = str(device_match.values[0])
                else:
                    device.numeric_id = str(device_match.values[0])

            if device.proxy_id and device.numeric_id:
                break

        if not device:
            print(f"[WARNING] Device not found in site model and device discovery: {asset}, {code}. Make sure the device is registered in the Cloud. Skipping.")
            continue

        if not (device.proxy_id and device.numeric_id):
            print(f"[WARNING] Could not find proxy_id and cloud_device_id for {asset}. Make sure the device is registered in the Cloud. Skipping.")
            continue

        existing_entity = existing_building_config.get_entity_by_num_id(device.numeric_id)

        if existing_entity:
            new_entity = dbo_models.Entity(
                guid=existing_entity.guid,
                code=existing_entity.code,
                etag=existing_entity.etag,
                proxy_id=device.proxy_id,
                cloud_device_id=device.numeric_id,
                type=type,
                display_name=display_name
            )
            new_entity.add_fields_from_dict(fields)
            new_entity.add_operation_flags(existing_entity)
        else:
            new_entity = dbo_models.Entity(
                code=code,
                proxy_id=device.proxy_id,
                cloud_device_id=device.numeric_id,
                type=type,
                display_name=display_name,
                operation="ADD"
            )
            new_entity.add_fields_from_dict(fields)


        new_building_config.add_entity(new_entity)

    helpers.write_yaml(output_path, new_building_config.to_dict())

    print(f"Building config successfully exported: {output_path}")


if __name__ == "__main__":
    main()