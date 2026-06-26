import os
import json
import copy
import uuid

from helpers import helpers
from models import dbo_models, cloud_models


def main():
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    building_config_path = input("Insert path to building config: ")
    site_model_path = input("Insert path to site model: ")
    bacnet_scan_path = input("[Optional] Insert path to bacnet scan (.xlsx) or press Enter to continue: ")

    if any([not device_discovery_path, 
        not building_config_path, 
        not site_model_path]):
        raise ValueError("Necessary inputs are missing.")

    device_discovery = helpers.load_file(device_discovery_path)
    carson_config = dbo_models.Site.from_config(building_config_path)
    site_model = cloud_models.SiteModel.from_dir(site_model_path)

    if bacnet_scan_path:
        bacnet_scan = helpers.load_file(bacnet_scan_path, sheet_name='devices')
        bacnet_scan = bacnet_scan[["device_name", "device_model", "device_serial_number"]].drop_duplicates().set_index("device_name").fillna("").T.to_dict()
    else:
        bacnet_scan = {}

    if "udmi" not in site_model_path:
        site_model_path = os.path.join(site_model_path, "udmi")
    if "devices" not in site_model_path:
        site_model_path = os.path.join(site_model_path, "devices")

    for d in os.listdir(site_model_path):

        item_path = os.path.join(site_model_path, d)
        print(item_path)

        # Skip files and specific exclusions
        if os.path.isfile(item_path) or "bacnet" in d:
            continue

        metadata_path = os.path.join(item_path, "metadata.json")
        if not os.path.exists(metadata_path):
            print(f"metadata.json not found in {d}")
            continue

        metadata = helpers.load_file(metadata_path)

        
        device = cloud_models.Device.from_metadata(d, metadata)

        # ------ AUGMENTATION DATA ------
        discovery_match = device_discovery.loc[device_discovery["device_id"] == device.proxy_id, "device_num_id"]
        
        if discovery_match.empty: 
            print(f"Discovery wasn't found: {item_path}. Skipping.")
            continue

        cloud_num_id = str(discovery_match.iloc[0])


        if "CGW" in d: # gateways have their own metadata and are not in carson config
            augmented_metadata = copy.deepcopy(device.metadata)

            if "cloud" not in augmented_metadata:
                augmented_metadata["cloud"] = {}

            augmented_metadata["cloud"]["resource_type"] = "GATEWAY"
            augmented_metadata["cloud"]["num_id"] = cloud_num_id

            if "system" not in augmented_metadata:
                augmented_metadata["system"] = {}
            augmented_metadata["system"]["tags"] =  [
                                                      "mango",
                                                      "virtual",
                                                      "gateway"
                                                    ]

            if "physical_tag" not in augmented_metadata["system"]:
                augmented_metadata["system"]["physical_tag"] = {"asset": {}}

            augmented_metadata["system"]["node_type"] = "virtual_device"
            if "guid" not in augmented_metadata["system"]["physical_tag"]["asset"]:
                augmented_metadata["system"]["physical_tag"]["asset"]["guid"] = f"uuid://{str(uuid.uuid4())}"
            augmented_metadata["system"]["physical_tag"]["asset"]["site"] = carson_config.code
            augmented_metadata["system"]["physical_tag"]["asset"]["name"] = augmented_metadata.get("localnet", {}).get("iot", {}).get("addr") or d

            with open(metadata_path, "w", encoding='utf-8') as f:
                json.dump(augmented_metadata, f, indent=2)

            continue


        entity = carson_config.get_entity_by_num_id(str(cloud_num_id))

        if not entity: 
            print(f"Entity wasn't found: {item_path}, cloud num_id: {cloud_num_id}. Skipping.")
            continue

        
        is_meter = True if "meter" in entity.code or "utility" in entity.code else False

        physical_tag_asset_guid = f"uuid://{entity.guid}"
        physical_tag_asset_name = device.proxy_id

        if is_meter:
            families_bacnet_addr = ""
            families_bacnet_network = ""
            system_name = entity.code.replace("utility-", "").replace("power-meter-", "")
            system_description = entity.code
            system_hardware_make = ""
            system_hardware_model = ""
            system_serial_no = ""
        else:
            families_bacnet_addr = device.device_index[0]
            families_bacnet_network = families_bacnet_addr[:5]
            system_name = entity.code
            system_description = f"bacnet-{families_bacnet_addr}"
            system_hardware_make = "ALC"
            system_hardware_model = bacnet_scan.get(f"device{families_bacnet_addr}", {}).get("device_model", {}) or ""
            system_serial_no = bacnet_scan.get(f"device{families_bacnet_addr}", {}).get("device_serial_number") or ""

        # tags:
        system_tags = metadata.get("system", {}).get("tags") or []
        if is_meter:
            if all(["modbus" not in system_tags,
                    "meter" not in system_tags,
                    "serial" not in system_tags]):
                system_tags.extend(["modbus",
                                  "meter",
                                  "serial"])
            if "EM" in device.proxy_id and "electricity" not in system_tags:
                system_tags.append("electricity")
            if "WM" in device.proxy_id and "water" not in system_tags:
                system_tags.append("water")
            if "GM" in device.proxy_id and "gas" not in system_tags:
                system_tags.append("gas")
            if "PVI" in device.proxy_id:
                pass
            if "main" in entity.code.lower() and "main" not in system_tags:
                system_tags.append("main")
        else:
            if all(["bacnet" not in system_tags,
                    "hvac" not in system_tags,
                    "serial" not in system_tags]):
                system_tags.extend(["bacnet",
                                  "hvac",
                                  "serial"])

        # -------------------------------

        print(f"""
Augmenting {item_path} with following information:
    device: {device.proxy_id}
    system/description: {system_description}
    system/name: {system_name}
    system/serial_no: {system_serial_no}
    system/hardware/make: {system_hardware_make}
    system/hardware/model: {system_hardware_model}
    physical_tag/asset/guid: {physical_tag_asset_guid}
    physical_tag/asset/site: {carson_config.code}
    physical_tag/asset/name: {physical_tag_asset_name}
    cloud/num_id: {cloud_num_id}
    families/bacnet/addr: {families_bacnet_addr}
    families/bacnet/network: {families_bacnet_network}
    units (not printed)
        """)

        augmented_metadata = copy.deepcopy(device.metadata)

        if "system" not in augmented_metadata:
            augmented_metadata["system"] = {}

        augmented_metadata["system"]["name"] = system_name or ""
        augmented_metadata["system"]["description"] = system_description
        augmented_metadata["system"]["serial_no"] = system_serial_no
        augmented_metadata["system"]["tags"] = system_tags

        if "hardware" not in augmented_metadata["system"]:
            augmented_metadata["system"]["hardware"] = {}

        augmented_metadata["system"]["hardware"]["make"] = system_hardware_make
        augmented_metadata["system"]["hardware"]["model"] = system_hardware_model

        if "physical_tag" not in augmented_metadata["system"]:
            augmented_metadata["system"]["physical_tag"] = {"asset": {}}
        
        augmented_metadata["system"]["physical_tag"]["asset"]["guid"] = physical_tag_asset_guid
        augmented_metadata["system"]["physical_tag"]["asset"]["site"] = carson_config.code
        augmented_metadata["system"]["physical_tag"]["asset"]["name"] = physical_tag_asset_name

        if "cloud" not in augmented_metadata:
            augmented_metadata["cloud"] = {}
        augmented_metadata["cloud"]["num_id"] = cloud_num_id or ""

        if "localnet" not in augmented_metadata:
            augmented_metadata["localnet"] = {}
        
        if "families" not in augmented_metadata["localnet"]:
            augmented_metadata["localnet"]["families"] = {}

        if not is_meter:
            if "bacnet" not in augmented_metadata["localnet"]["families"]:
                augmented_metadata["localnet"]["families"]["bacnet"] = {}

            augmented_metadata["localnet"]["families"]["bacnet"]["addr"] = families_bacnet_addr
            augmented_metadata["localnet"]["families"]["bacnet"]["network"] = families_bacnet_network

        # add units by field from Carson model
        for k, v in augmented_metadata["pointset"]["points"].items():
            v["units"] = entity.get_units_by_field_name(k) or ""

        with open(metadata_path, "w", encoding='utf-8') as f:
            json.dump(augmented_metadata, f, indent=2)


if __name__ == "__main__":
    main()