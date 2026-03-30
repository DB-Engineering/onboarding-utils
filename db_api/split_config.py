import os
import yaml


def split_yaml_configs(yaml_file):
    # === Load YAML ===
    with open(yaml_file, "r") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Top-level YAML structure must be a dictionary")

    # === Detect building GUID ===
    building_guid_block = None
    for guid, content in data.items():
        if isinstance(content, dict) and content.get("type") == "FACILITIES/BUILDING":
            building_guid_block = {guid: content}
            break

    # === Output directory ===
    base_dir = os.path.dirname(os.path.abspath(yaml_file))
    output_dir = os.path.join(base_dir, "split_configs")
    os.makedirs(output_dir, exist_ok=True)

    created_count = 0

    # === Split into individual files ===
    for _, (guid, content) in enumerate(data.items(), start=1):
        try:
            # Skip CONFIG_METADATA and building GUID itself
            if guid == "CONFIG_METADATA" or (building_guid_block and guid in building_guid_block):
                continue

            if not isinstance(content, dict):
                continue

            # === Build output data ===
            output_data = {
                "CONFIG_METADATA": {"operation": "UPDATE"}
            }

            if building_guid_block:
                output_data.update(building_guid_block)

            output_data[guid] = content

            output_file = os.path.join(output_dir, f"config_{created_count+1}.yaml")
            with open(output_file, "w") as f:
                yaml.dump(output_data, f, sort_keys=False)

            created_count += 1

        except Exception as e:
            print(f"⚠️ Skipping {guid}: {e}")

    print(f"✅ Done. Created {created_count} files in '{output_dir}'.")


# === Run ===
if __name__ == "__main__":
    yaml_path = input("Enter path to input YAML file: ").strip()
    split_yaml_configs(yaml_path)