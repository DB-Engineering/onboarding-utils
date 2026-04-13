from abc import ABC, abstractmethod
import pandas as pd
import uuid

from helpers import helpers

class Field(ABC):
    def __init__(self, field_name):
        self.dbo_field_name = field_name

class UnitField(Field):
    def __init__(self, field_name, dbo_unit):
        super().__init__(field_name)
        self.dbo_unit = dbo_unit.replace("-", "_")

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

    def to_dict(self):
        return {
            "present_value": f"points.{self.dbo_field_name}.present_value",
            "states": self.dbo_states
        }

class MissingField(Field):
    def __init__(self, field_name):
        super().__init__(field_name)

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
        try:
            for k, v in fields.items():
                if k not in self.fields:
                    obj_type = v.get("objectType")

                    if obj_type in ("AI", "AO", "AV"):
                        self.fields.append(
                                UnitField(
                                    field_name=k,
                                    dbo_unit=helpers.map_units(k)
                                )
                            )
                    elif obj_type in ("BI", "BO", "BV", "MSV"):
                        self.fields.append(
                            StateField(
                                    field_name=k,
                                    dbo_states=helpers.map_states(k)
                                )
                            )
                    elif v.get("isMissing")=="YES":
                        self.fields.append(
                            MissingField(field_name=k)
                            )
                    else:
                        raise ValueError(f"[ERROR] {k}: unknown objectType: {obj_type}")
                        continue
        except Exception as e:
            print(f"[ERROR] Unable to add field: {k} due to: {e}")
            return False

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
                operation=None
                )

            site.entities.append(entity)
            site._cloud_id_to_entity_map[val.get("cloud_device_id")] = entity

        return site

    def get_entity_by_num_id(self, num_id):
        return self._cloud_id_to_entity_map.get(str(num_id))



