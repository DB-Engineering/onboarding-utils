from abc import ABC, abstractmethod
import pandas as pd
import uuid

from helpers import helpers

class Field(ABC):
    def __init__(self, field_name):
        self.dbo_field_name = field_name

    @abstractmethod
    def get_units(self):
        pass

class UnitField(Field):
    def __init__(self, field_name, dbo_unit):
        super().__init__(field_name)
        self.dbo_unit = dbo_unit.replace("-", "_")

    def get_units(self):
        return self.dbo_unit

    def to_dict(self):
        return {
                    "present_value": f"points.{self.dbo_field_name}.present_value",
                    "units": {
                        "key": f"pointset.points.{self.dbo_field_name}.unit",
                        "values": {
                            self.dbo_unit: self.dbo_unit.replace("_", "-")
                        }
                    }
                }

class StateField(Field):
    def __init__(self, field_name, dbo_states: dict):
        super().__init__(field_name)
        self.dbo_states = dbo_states

    def get_units(self):
        return "no_units"

    def to_dict(self):
        return {
            "present_value": f"points.{self.dbo_field_name}.present_value",
            "states": self.dbo_states
        }

class MissingField(Field):
    def __init__(self, field_name):
        super().__init__(field_name)

    def get_units(self):
        return None

    def to_dict(self):
        return "MISSING"

class Entity():
    def __init__(self, 
        guid=None,
        code=None,
        etag=None,
        proxy_id=None,
        cloud_device_id=None, 
        namespace=None,
        type_name=None,
        display_name=None,
        fields = None,
        operation=None):
        self.guid = guid or str(uuid.uuid4())
        self.code = code
        self.etag = etag
        self.proxy_id = proxy_id
        self.cloud_device_id = cloud_device_id
        self.namespace = namespace
        self.type_name = type_name
        self.display_name=display_name
        self.fields = []
        self.operation = None

    def add_fields_from_dict(self, fields: dict):
        """
        Input: loadsheet slice:
        loadsheet_slice - slice of loadsheet containing fields of a single asset.
        fields = loadsheet_slice.set_index("standardFieldName", drop=True)[["units", "deviceId", "objectType", 
                                                                            "objectId", "isMissing"]].T.to_dict()
        """
        try:
            new_fields = []
            seen_keys = set()
            for k, v in fields.items():
                if k in seen_keys:
                    continue

                obj_type = v.get("objectType")

                if obj_type in ("AI", "AO", "AV"):
                    new_fields.append(
                            UnitField(
                                field_name=k,
                                dbo_unit=helpers.map_units(k)
                            )
                        )
                    seen_keys.add(k)
                elif obj_type in ("BI", "BO", "BV", "MSV"):
                    new_fields.append(
                        StateField(
                                field_name=k,
                                dbo_states=helpers.map_states(k)
                            )
                        )
                    seen_keys.add(k)
                elif v.get("isMissing")=="YES":
                    new_fields.append(
                        MissingField(field_name=k)
                        )
                    seen_keys.add(k)
                else:
                    raise ValueError(f"[ERROR] {k}: unknown objectType: {obj_type}")
                    continue
            self.fields = new_fields

        except Exception as e:
            print(f"[ERROR] Unable to add field: {k} due to: {e}")
            return []

    def add_fields_from_translation(self, translation: dict):
        """
        Input: translation from carson entity config.
        """
        if not translation:
            return []
        try:
            new_fields = []
            seen_keys = set()
            for k, v in translation.items():

                if k in seen_keys:
                    continue

                field_name = k.split(".")[1] if "." in k else k

                if "units" in v:
                    new_fields.append(
                            UnitField(
                                field_name = field_name,
                                dbo_unit=list(v["units"]["values"].keys())[0]
                            )
                        )
                if "states" in v:
                    new_fields.append(
                        StateField(
                                field_name=field_name,
                                dbo_states=v["states"]
                            )
                        )
                seen_keys.add(k)

            self.fields = new_fields

        except Exception as e:
            print(f"[ERROR] Unable to add field: {k} due to: {e}")
            return []

    def get_units_by_field_name(self, field_name):
        for field in self.fields:
            if field.dbo_field_name == field_name:
                return field.get_units()
        return None

    def to_dict(self):
        return {
                    str(self.guid): {
                        "cloud_device_id": self.cloud_device_id,
                        "display_name": self.display_name,
                        "code": self.code,
                        "type": f"{self.namespace}/{self.type_name}",
                        "operation": self.operation or "ADD",
                        "translation": {field.dbo_field_name: field.to_dict() for field in self.fields}
                    }
                }

class Site():
    def __init__(self, name, guid):
        self._name = name
        self.guid = guid
        self.entities = []
        self._cloud_id_to_entity_map = {}

    @property
    def name(self):
        return self._name

    @classmethod
    def from_config(cls, config_path: str):
        config = helpers.load_file(config_path)
        if not isinstance(config, dict):
            raise TypeError("Config must be a dictionary.")

        # site entity can be anywhere in the config so first pass to find site
        for key, val in config.items():
            if val.get("type")=='FACILITIES/BUILDING':
                site = cls(
                    name=val.get('code'),
                    guid=key
                    )

        for key, val in config.items():
            if key == "CONFIG_METADATA":
                continue
            if val.get("type")=='FACILITIES/BUILDING':
                continue

            entity = Entity(
                guid=key,
                code=val.get("code"),
                etag=val.get("etag"),
                proxy_id=None,
                cloud_device_id=str(val.get("cloud_device_id")), 
                namespace=val.get("type").split("/")[0] if val.get("type") else None,
                type_name=val.get("type"),
                display_name=val.get("display_name"),
                fields=val.get("translation", {}),
                operation=None
                )

            site.entities.append(entity)
            site._cloud_id_to_entity_map[str(val.get("cloud_device_id"))] = entity

        return site

    def get_entity_by_num_id(self, num_id):
        return self._cloud_id_to_entity_map.get(str(num_id))



