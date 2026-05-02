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

    def __eq__(self, other):
        if not isinstance(other, Field):
            return False
        return self.__dict__ == other.__dict__

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
        type=None,
        display_name=None,
        translation = None,
        operation=None):
        self._guid = guid or str(uuid.uuid4())
        self.code = code
        self._etag = etag
        self.proxy_id = proxy_id
        self.cloud_device_id = cloud_device_id
        self.type = type
        self.display_name=display_name
        self.translation = []
        self._operation = operation
        self.update_mask = None

    @property
    def etag(self):
        return self._etag

    @etag.setter
    def etag(self, value):
        self._etag = value

    @property
    def guid(self):
        return self._guid

    @guid.setter
    def guid(self, value):
        self._guid = value

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        self._operation = value

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
            self.translation = new_fields

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

            self.translation = new_fields

        except Exception as e:
            print(f"[ERROR] Unable to add field: {k} due to: {e}")
            return []

    def get_units_by_field_name(self, field_name):
        for field in self.translation:
            if field.dbo_field_name == field_name:
                return field.get_units()
        return None

    def add_operation_flags(self, other):
        """
        Compares self to another instance and returns a list of 
        attribute names that differ between the two.
        """
        if not isinstance(other, self.__class__):
            raise ValueError("Comparison must be between instances of the same class.")

        mask_fields = [
                "display_name",
                "translation",
                "type"
            ]

        update_mask = []

        for field in mask_fields:
            if getattr(self, field) != getattr(other, field):
                update_mask.append(field)

        if update_mask:
            self._operation = "UPDATE"

        self.update_mask = update_mask

    def to_dict(self):

        device_data = {
                "cloud_device_id": self.cloud_device_id,
                "display_name": self.display_name,
                "code": self.code,
                "type": self.type,
                "translation": {field.dbo_field_name: field.to_dict() for field in self.translation}
            }

        if self.etag:
                device_data["etag"] = self.etag

        if self.operation:
                device_data["operation"] = self.operation

        if self.update_mask:
                device_data["update_mask"] = self.update_mask

        return {str(self._guid): device_data}


class Site():
    def __init__(self, code, guid, type, etag=None):
        self._code = code
        self.guid = guid
        self.type = type
        self.etag = etag
        self.entities = []
        self._cloud_id_to_entity_map = {}

    @property
    def code(self):
        return self._code

    @classmethod
    def from_config(cls, config_path: str):
        config = helpers.load_file(config_path)
        if not isinstance(config, dict):
            raise TypeError("Config must be a dictionary.")

        # site entity can be anywhere in the config so first pass to find site
        for key, val in config.items():
            if val.get("type")=='FACILITIES/BUILDING':
                site = cls(
                    code=val.get("code"),
                    guid=key,
                    type=val.get("type"),
                    etag=val.get("etag")
                    )
                break

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
                type=val.get("type"),
                display_name=val.get("display_name"),
                operation=None
                )
            entity.add_fields_from_translation(val.get("translation", {}))

            site.entities.append(entity)
            site._cloud_id_to_entity_map[str(val.get("cloud_device_id"))] = entity

        return site

    def add_entity(self, new_entity: Entity):
        self.entities.append(new_entity)

    def get_entity_by_num_id(self, num_id):
        return self._cloud_id_to_entity_map.get(str(num_id))

    def to_dict(self):
        output = {
                "CONFIG_METADATA": {
                    "operation": "UPDATE"
                },
                self.guid: {
                    "code": self.code,
                    "etag": self.etag,
                    "type": self.type
                }
            }
            
        for entity in self.entities:
            output.update(entity.to_dict())
            
        return output



