import os

def split_config_file(input_file):
    with open(input_file, "r") as f:
        lines = f.readlines()

    # Detect CONFIG_METADATA (always at the top)
    config_metadata = []
    i = 0
    while i < len(lines):
        line = lines[i]
        config_metadata.append(line)
        if line.strip().startswith("operation: UPDATE"):
            i += 1
            break
        i += 1

    # Detect the FACILITIES/BUILDING block
    facility_block = []
    current_block = []
    facility_found = False

    while i < len(lines):
        line = lines[i]
        if not line.startswith(" ") and line.strip().endswith(":"):
            if current_block:
                if any("type: FACILITIES/BUILDING" in l for l in current_block):
                    facility_block = current_block[:]
                    facility_found = True
                    break
            current_block = [line]
        else:
            current_block.append(line)
        i += 1

    if not facility_found:
        raise ValueError("FACILITIES/BUILDING block not found in YAML file.")

    # Prepare header for every file
    header = config_metadata + facility_block

    # Collect all GUID blocks
    guid_blocks = []
    current_block = []

    while i < len(lines):
        line = lines[i]
        if not line.startswith(" ") and line.strip().endswith(":"):
            if current_block:
                guid_blocks.append(current_block)
            current_block = [line]
        else:
            current_block.append(line)
        i += 1
    if current_block:
        guid_blocks.append(current_block)

    # Directory for outputs
    output_dir = os.path.dirname(input_file)
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    # Write each GUID block into its own file
    part_num = 1
    for block in guid_blocks:
        output_file = os.path.join(output_dir, f"{base_name}_pt{part_num}.yaml")
        with open(output_file, "w") as f:
            f.writelines(header + block)
        print(f"Wrote {output_file} with {len(header) + len(block)} lines")
        part_num += 1


def main():
    try:
        input_file = input("Enter the absolute path to the original config file: ").strip()
        if not os.path.isabs(input_file):
            print("❌ Please provide an absolute path.")
        elif not os.path.exists(input_file):
            print("❌ File not found at that path.")
        else:
            split_config_file(input_file)
    except Exception as e:
        print(f"[ERROR] Unable to split your config: {e}")

if __name__ == "__main__":
    main()