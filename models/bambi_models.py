from abc import ABC, abstractmethod
import pandas as pd
import uuid

from helpers import helpers

class Point():
    def __init__(self, points_template_name=None, point_name=None, units=None, ref=None, writable="false"):
        self.point_name = point_name,
        self.units = units,
        self.ref = ref

    def to_dict(self):
        return {
                    f"{self.point_name}": {
                        "units": self.units,
                        "writable": False,
                        "ref": self.ref
                    }
                }

class BAMBIDevice():
    def __init__(self, proxy_id=None, system_name=None, bacnet_ids=None, networks=None):
        self._proxy_id = proxy_id
        self.system_name = system_name # control program
        self.bacnet_ids=bacnet_ids # list[str]
        self.networks=networks # list[str]
        self.points = []

    @property
    def proxy_id(self):
        return self._proxy_id
    
    @proxy_id.setter
    def proxy_id(self, value):
        if not isinstance(value, str) and value is not None:
            raise ValueError("proxy_id must be a string or None")
        self._proxy_id = value

    def add_points_from_dict(self, fields: dict):
        """
        Input: loadsheet slice:
        loadsheet_slice - slice of loadsheet containing fields of a single asset.
        fields = loadsheet_slice.set_index("standardFieldName", drop=True)[["units", "deviceId", "objectType", 
                                                                            "objectId", "isMissing"]].T.to_dict()
        """
        try:
            new_points = []
            for k, v in fields.items():

                device_id = v.get("deviceId")
                obj_type = v.get("objectType")
                obj_id = v.get("objectId")


                if obj_type in ("AI", "AO", "AV"):
                    new_points.append(
                            Point(
                                points_template_name = k,
                                units = v.get("units").replace("-", "_"),
                                ref = helpers.object_id_to_xid(device_id, obj_type, obj_id)
                            )
                        )
                elif obj_type in ("BI", "BO", "BV", "MSV"):
                    new_points.append(
                        Point(
                                points_template_name = k,
                                units = "no_units",
                                ref = helpers.object_id_to_xid(device_id, obj_type, obj_id)
                            )
                        )
                else:
                    raise ValueError(f"[ERROR] {k}: unknown objectType: {obj_type}")
                    continue
            self.points = new_points
        except Exception as e:
            print(f"Couldn't add points due to exception: {e}")