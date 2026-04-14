import os
from helpers import helpers

def parse_object_id(dp_string):
    """
    Parses device_id and object_id from a metadata string.
    Example:
    Input:          'DP_2800039_ANALOG_VALUE_68'.
    Returned tuple: ('2800039', 'AV:68')
    """
    try:
        parts = dp_string.split("_")
        numeric_id = parts[1]
        type_initials = "".join([word[0] for word in parts[2:-1]])
        index = parts[-1]
        return str(numeric_id), f"{type_initials}:{index}"
    except Exception as e:
        # print(f"[WARNING] Unknown XID format: {dp_string}")
        return None, None

class Device:
    def __init__(self, proxy_id=None, numeric_id=None, point_dict=None, device_list=None, metadata=None):
        self._proxy_id = proxy_id
        self._numeric_id = numeric_id
        self._metadata = metadata
        self.point_index = point_dict or {}
        self._device_index = device_list or []

    def __repr__(self):
        return (f"proxy_id: {self._proxy_id}, numeric_id: {self._numeric_id}")

    @property
    def proxy_id(self):
        return self._proxy_id
    
    @proxy_id.setter
    def proxy_id(self, value):
        if not isinstance(value, str) and value is not None:
            raise ValueError("proxy_id must be a string or None")
        self._proxy_id = value

    @property
    def numeric_id(self):
        return self._numeric_id
    
    @numeric_id.setter
    def numeric_id(self, value):
        if not isinstance(value, str) and value is not None:
            raise ValueError("numeric_id must be a string or None")
        self._numeric_id = value

    @property
    def device_index(self):
        return self._device_index

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        if not isinstance(value, dict) and value is not None:
            raise ValueError("numeric_id must be a dictionary or None")
        self._metadata = value

    def get_field_name_by_object_id(self, device_id: str, object_id: str):
        """
        Takes in device_id (3002201) and object_id (AV:1) and returns field name associated with this object in the device metadata.
        """
        return self.point_index.get(f"{device_id}:{object_id}")

    @classmethod
    def from_metadata(cls, name, metadata_dict):
        """Creates a Device instance from a dictionary."""
        num_id = metadata_dict.get("cloud", {}).get("num_id")
        if not num_id:
            # print(f"[WARNING] Device numeric id not found for {name} in site model.")
            pass

        points_found = {}
        devices_found = []
        points_data = metadata_dict.get("pointset", {}).get("points") or {}
        for k, v in points_data.items():
            device_id, object_id = parse_object_id(v.get("ref"))
            if not (device_id and object_id):
                continue

            if not device_id in devices_found:
                devices_found.append(device_id)

            point_id = f"{device_id}:{object_id}"
            if not point_id in points_found:
                points_found[point_id] = k
            
        return cls(
                proxy_id=name,
                numeric_id=num_id,
                point_dict=points_found,
                device_list=devices_found,
                metadata=metadata_dict
                )

class SiteModel():
    def __init__(self):
        self.devices = {}
        self._point_to_device_map = {}
        self._duplicate_points = set()

    def add_device(self, device: Device):
        if device.proxy_id in self.devices:
            print(f"Device already added: {device.proxy_id}")
            return
        self.devices[device.proxy_id] = device

        if not device.point_index:
            return
        for pt in device.point_index:
            # add any shared points in a separate index
            if pt in self._point_to_device_map:
                self._duplicate_points.add(pt)
                del self._point_to_device_map[pt]
            elif pt not in self._duplicate_points:
                self._point_to_device_map[pt] = device

    def get_device_by_proxy_id(self, proxy_id):
        return self.devices.get(proxy_id)

    def get_device_by_object_id(self, device_id: str, object_id: str):
        """Finds the device containing a specific, non-shared BACnet object."""
        clean_dev = str(device_id).replace("DEV:", "")
        lookup_key = f"{clean_dev}:{object_id}"

        if lookup_key in self._duplicate_points:
            return None
        else:
            return self._point_to_device_map.get(lookup_key)

    @classmethod
    def from_dir(cls, path):
        """Factory method to build a SiteModel from a directory."""
        if not os.path.exists(path):
            print(f"Directory not found: {path}")
            return None

        if "udmi" not in path:
            path = os.path.join(path, "udmi")
        if "devices" not in path:
            path = os.path.join(path, "devices")

        site = cls()
        
        if not os.path.exists(path):
            print(f"Directory not found: {path}")
            return site

        for d in os.listdir(path):
            item_path = os.path.join(path, d)
            # print(f"Processing {item_path}")
            # Skip files and specific exclusions
            if os.path.isfile(item_path) or any(x in d for x in ["bacnet", "CGW"]):
                continue

            metadata_path = os.path.join(item_path, "metadata.json")
            if not os.path.exists(metadata_path):
                print(f"metadata.json not found in {d}")
                continue

            metadata = helpers.load_file(metadata_path)
            
            try:
                site.add_device(
                    Device.from_metadata(d, metadata)
                    )
            except ValueError as e:
                print(e)
                
        return site