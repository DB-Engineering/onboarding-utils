import os
import re
import json
import copy

from helpers import helpers, type_matcher
from models import dbo_models, cloud_models

def main():
    site_model_path = input("Insert path to site model: ")
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    building_config_path = input("Insert path to building config: ")
    output_path = input("Insert path for output building config file (.yaml): ")

    if any([not site_model_path,
            not device_discovery_path,
            not building_config_path,
            not output_path]):
        raise ValueError("[ERROR] Necessary inputs are missing.")

    site_model = cloud_models.SiteModel.from_dir(site_model_path)
    device_discovery = helpers.load_file(device_discovery_path)
    building_config = dbo_models.Site.from_config(building_config_path)
    new_building_config = dbo_models.Site(
        code=building_config.code,
        guid=building_config.guid,
        type=building_config.type,
        etag=building_config.etag
        )

    for device_name, device in site_model.devices.items():

        meter_type = device.proxy_id.split( "-")[0]
        if meter_type == "PVI":
            meter_type = "EM"
        if not meter_type in ("EM", "WM", "GM", "PVI"):
            continue
        print(f"\nProcessing {device_name}")

        print(f"guid site model: {device.guid}")

        if not device.numeric_id:
            device_match = device_discovery.loc[device_discovery.device_id==device.proxy_id, 'device_num_id']
            if device_match.empty:
                print(f"[WARNING] No numeric_id found for proxy_id: {device.proxy_id}")
                continue
            elif len(device_match) > 1:
                print(f"[WARNING] Multiple numeric_id found for {device.proxy_id}, {', '.join(device_match.tolist())}. Using the first one.")
                continue
            else:
                device.numeric_id = str(device_match.values[0])

        # --------- ENTITY DATA ----------

        # infer code
        points = device.metadata.get("pointset", {}).get("points")
        if "inverter" in json.dumps(points).lower(): # skip inverters
            print("Skipping inverter.")
            continue

        refs = []
        for k, v in points.items():
            if k.lower() != "ping" and v.get("ref"):
                refs.append(v.get("ref"))
                if len(refs) == 4: break

        if len(refs) == 0:
            continue
        
        code = helpers.get_common_prefix(refs)

        if len(code) < 3:
            print(f"Not able to extract meter name from references: {refs[0]}.")
            code = input("Enter meter name manually: ")
        else:
            if "EM" in device.proxy_id:
                idx = code.find("EM")
                code = "power-meter-"+code[idx:]
            if "PVI" in device.proxy_id:
                idx = code.find("PV")
                code = "power-meter-"+code[idx:]
            if "WM" in device.proxy_id or "GM" in device.proxy_id:
                idx = code.find("WM")
                code = "utility-"+code[idx:]
            if "GM" in device.proxy_id:
                idx = code.find("GM")
                code = "utility-"+code[idx:]

        print(f"Suggested meter code: {code}. (field reference: {refs[0]})")
        code_propmt = input("Press Enter to continue with this code or insert your own code:")
        if code_propmt != "":
            code = code_propmt

        # Interactive type matching
        fields = set(points.keys())
        suggested_type, pre_add_fields = type_matcher.run_type_matcher(fields, meter_type)
        type_name = "METER/"+type_matcher.get_type_name(suggestion=suggested_type)
        print(f"Final meter type: {type_name}.")
        type_propmt = input("Press Enter to continue with this type or insert your own type (without a namespace):")
        if type_propmt != "":
            type_name = "METER/"+type_propmt
        # --------------------------------

        existing_entity = building_config.get_entity_by_num_id(device.numeric_id)

        if existing_entity:
            new_entity = dbo_models.Entity(
                guid=existing_entity.guid,
                code=existing_entity.code,
                etag=existing_entity.etag,
                connections=existing_entity.connections,
                proxy_id=device.proxy_id,
                cloud_device_id=device.numeric_id,
                type=type_name,
                display_name=existing_entity.code,
            )
            new_entity.add_fields_from_metadata(device.metadata.get("pointset", {}).get("points"))
            new_entity.add_operation_flags(existing_entity)

        else:
            new_entity = dbo_models.Entity(
                guid=device.guid,
                code=code,
                proxy_id=device.proxy_id,
                cloud_device_id=device.numeric_id,
                type=type_name,
                display_name=code,
                operation="ADD"
            )
            new_entity.add_fields_from_metadata(device.metadata.get("pointset", {}).get("points"))

        new_building_config.add_entity(new_entity)

    helpers.write_yaml(output_path, new_building_config.to_dict())

    print(f"Building config successfully exported: {output_path}")