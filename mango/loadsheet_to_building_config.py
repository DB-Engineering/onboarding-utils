from helpers import helpers
from models import cloud_models, dbo_models
import sys

def main():
    loadsheet_path = input("Insert path to loadsheet (.xlsx): ")
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    site_model_path = input("Insert path to site model: ")
    output_path = input("Insert path for output building config file (.yaml): ")

    if any([not loadsheet_path, not device_discovery_path, not site_model_path, not output_path]):
        raise ValueError("Necessary inputs are missing.")

    device_discovery = helpers.load_file(device_discovery_path)
    site_model = cloud_models.SiteModel.from_dir(site_model_path)

    loadsheet = helpers.load_file(loadsheet_path)
    loadsheet = loadsheet.loc[loadsheet["required"]=="YES"]

    assets = loadsheet["assetName"].unique().tolist()

    building_config = {}
    building_config.update(
        {"CONFIG_METADATA": {"operation": "UDATE"}}
        )

    for asset in assets:
        asset_loadsheet = loadsheet.loc[loadsheet["assetName"]==asset, ["controlProgram", "typeName", "assetName", "standardFieldName", 
                                                                        "units", "deviceId", "objectType", "objectId", "isMissing"]].astype(str)
        display_name = asset
        code = ", ".join(sorted(asset_loadsheet.controlProgram.unique().tolist()))
        namespace = "HVAC"  # default HVAC for now
        type_name = asset_loadsheet.typeName.unique().tolist()[0]
        operation = "ADD"

        asset_loadsheet.set_index("standardFieldName", drop=True)[["deviceId", "objectType", "objectId", "isMissing"]].T.to_dict()

        fields = asset_loadsheet.set_index("standardFieldName", drop=True)[["units", "deviceId", "objectType", 
                                                                            "objectId", "isMissing"]].T.to_dict()

        # get proxy_id and cloud_device_id by objectId
        for k, v in fields.items():
            if v.get("isMissing") == "YES":
                continue
            device = site_model.get_device_by_object_id(
                                    v.get("deviceId"), 
                                    f"{v.get('objectType')}:{v.get('objectId')}"
                                    )
            if not device: 
                continue
            elif not device.numeric_id:
                device.numeric_id = device_discovery.loc[device_discovery.device_id==proxy_id, 'device_num_id'].item()

            if device.proxy_id and device.numeric_id:
                break

        if not (device.proxy_id and device.numeric_id):
            print(f"[ERROR] Could not find proxy_id and cloud_device_id for {asset}")
        
        entity = dbo_models.Entity(
            code=code,
            proxy_id=device.proxy_id,
            cloud_device_id=device.numeric_id, 
            namespace=namespace,
            type_name=type_name,
            display_name=display_name,
            operation=operation
            )

        entity.add_fields_from_dict(fields)

        building_config.update(entity.to_dict())

        helpers.write_yaml(output_path, building_config)

        print(f"Building config successfully exported: {output_path}")

if __name__=="__main__":
    main()