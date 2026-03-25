import yaml

# Helper to force single-quoted YAML string
class SingleQuoted(str):
    pass

def single_quoted_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")

yaml.add_representer(SingleQuoted, single_quoted_representer)

def sync_etags(full_building_config_file, target_file):
    # Load both YAMLs
    with open(full_building_config_file, 'r') as f:
        full_building_data = yaml.safe_load(f)

    with open(target_file, 'r') as f:
        target_data = yaml.safe_load(f)

    total_entities = 0
    updated_count = 0

    # Walk through UUIDs
    for uuid, target_entity in target_data.items():
        if uuid == "CONFIG_METADATA":
            continue
        total_entities += 1
        if uuid in full_building_data and "etag" in full_building_data[uuid]:
            target_entity["etag"] = SingleQuoted(str(full_building_data[uuid]["etag"]))
            updated_count += 1
        elif uuid not in full_building_data:
            print(f"Warning: UUID {uuid} not found in full building config file")

    # Overwrite the original target file
    with open(target_file, 'w') as f:
        yaml.dump(target_data, f, sort_keys=False)

    # Post-process: replace true/false with ON/OFF
    with open(target_file, 'r') as f:
        content = f.read()
    content = content.replace("true", "ON").replace("false", "OFF")
    with open(target_file, 'w') as f:
        f.write(content)

    print(f"Processed {total_entities} entities in config file, successfully updated {updated_count} etags")