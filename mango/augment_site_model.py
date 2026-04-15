import os
import json
import copy

from helpers import helpers
from models import dbo_models, cloud_models


def main():
    device_discovery_path = input("Insert path to device discovery (.csv): ")
    building_config_path = input("Insert path to building config: ")
    site_model_path = input("Insert path to site model: ")

    if any([not device_discovery_path, 
        not building_config_path, 
        not site_model_path]):
        raise ValueError("Necessary inputs are missing.")

    device_discovery = helpers.load_file(device_discovery_path)
    carson_config = dbo_models.Site.from_config(building_config_path)
    site_model = cloud_models.SiteModel.from_dir(site_model_path)

    if not os.path.exists(site_model_path):
        print(f"Directory not found: {site_model_path}")
        return None

    if "udmi" not in site_model_path:
        site_model_path = os.path.join(site_model_path, "udmi")
    if "devices" not in site_model_path:
        site_model_path = os.path.join(site_model_path, "devices")
    
    if not os.path.exists(site_model_path):
        raise ValueError(f"Directory not found: {site_model_path}")

    for d in os.listdir(site_model_path):

        item_path = os.path.join(site_model_path, d)

        # Skip files and specific exclusions
        if os.path.isfile(item_path) or any(x in d for x in ["bacnet", "CGW"]):
            continue

        metadata_path = os.path.join(item_path, "metadata.json")
        if not os.path.exists(metadata_path):
            print(f"metadata.json not found in {d}")
            continue

        metadata = helpers.load_file(metadata_path)
        
        device = cloud_models.Device.from_metadata(d, metadata)

        # ------ AUGMENTATION DATA ------
        discovery_match = device_discovery.loc[device_discovery["device_id"] == device.proxy_id, "device_num_id"]

        if discovery_match.empty: continue

        cloud_num_id = discovery_match.item()
        entity = carson_config.get_entity_by_num_id(str(cloud_num_id))

        if not entity: continue

        physical_tag_asset_guid = f"uuid://{entity.guid}"
        system_name = entity.code
        system_description = ", ".join([f"bacnet:{i}" for i in device.device_index])
        # system_tags = ["bacnet", "hvac", "serial"]
        physical_tag_asset_name = device.proxy_id
        families_bacnet_addr = ", ".join([i for i in device.device_index])
        families_bacnet_network = ", ".join(set([i[:5] if all([isinstance(i, str), len(i) > 6]) else i for i in device.device_index]))
        # -------------------------------

        print(f"""
Augmenting {item_path} with following information:
    device: {device.proxy_id}
    system/description: {system_description}
    system/name: {system_name}
    physical_tag/asset/name: {physical_tag_asset_name}
    physical_tag/asset/guid: {physical_tag_asset_guid}
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
        # augmented_metadata["system"]["tags"] = system_tags

        if "physical_tag" not in augmented_metadata["system"]:
            augmented_metadata["system"]["physical_tag"] = {"asset": {}}
        
        augmented_metadata["system"]["physical_tag"]["asset"]["guid"] = physical_tag_asset_guid or ""
        augmented_metadata["system"]["physical_tag"]["asset"]["site"] = carson_config.name
        augmented_metadata["system"]["physical_tag"]["asset"]["name"] = physical_tag_asset_name

        if "cloud" not in augmented_metadata:
            augmented_metadata["cloud"] = {}
        augmented_metadata["cloud"]["num_id"] = cloud_num_id or ""

        if "localnet" not in augmented_metadata:
            augmented_metadata["localnet"] = {}
        
        if "families" not in augmented_metadata["localnet"]:
            augmented_metadata["localnet"]["families"] = {}
        if "bacnet" not in augmented_metadata["localnet"]["families"]:
            augmented_metadata["localnet"]["families"] = { "bacnet": {}}

        augmented_metadata["localnet"]["families"]["bacnet"]["addr"] = families_bacnet_addr
        augmented_metadata["localnet"]["families"]["bacnet"]["network"] = families_bacnet_network

        # add units by field from Carson model
        for k, v in augmented_metadata["pointset"]["points"].items():
            v["units"] = entity.get_units_by_field_name(k) or ""

        with open(metadata_path, "w", encoding='utf-8') as f:
            json.dump(augmented_metadata, f, indent=2)


if __name__ == "__main__":
    main()