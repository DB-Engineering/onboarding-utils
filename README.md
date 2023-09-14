# onboarding-utils
Tools for exporting onboard config files for DB API

## abel_convert_loadsheet.py

Use this file if you need to convert a legacy Loadsheet into ABEL spreadsheet.
This functionality is built into Loadboy (See #11 in [Loadboy README][1]) 

[1]: https://github.com/DB-Engineering/Google-Onboarding-Tool-19/tree/master#example-workflow

In case it's not working, use this import.
(Use this [Plx script template][2] to export Payload)
```
from abel_convert_loadsheet import *

loadsheet_path = './Loadsheet.xlsx'
payload_path = './Payload.csv'

convert = Abel()
convert.import_loadsheet(loadsheet_path)
convert.import_payload(payload_path)
convert.build()
convert.dump(loadsheet_path)
```
[2]: https://plx.corp.google.com/scripts2/script_46._66ecd6_a19c_4ed8_8a24_5baea50ff96a

## onboarding_utils.py
Contains the following functions:
* export_update_config
  
  Exports Onboard-Update config yaml. If there are more than 100 Entities to update, multiple config files will be exported to prevent DB API operation deadline errors.

  Arguments:

  * building_config_path: path to building config export
  * abel_config_path: path to ABEL config
  * dump_path: path to the new onboard-update yaml
  * entity_list (optional): list of Entity Guids, if only need to export a config for select Entities. Default: None, all entities will be exported.

  ```
  building_config = './building_export.yaml'
  abel_config = './abel_config.yaml'
  dump_path = './onboard_update_config.yaml'
  entity_list = ['54293735-44d1-4eb9-99af-8360ee80bdb8',
                '634c437f-e420-41c1-a9d4-86896b8b70b5',
                '6feafd10-06ed-4a18-b8a5-0fbea5af74db']
  
  export_update_config(building_config, abel_config, dump_path, entity_list)
  ```
  
* export_add_config

  Exports Onboard-Add config yaml.
  
  Arguments:
  * building_config_path: path to building config export (export a new building config after Onboard-Update operation!)
  * abel_config_path: path to ABEL config
  * dump_path: path to the new onboard-add yaml
 
  ```
  building_config = './building_export.yaml'
  abel_config = './abel_config.yaml'
  dump_path = './onboard_add_config.yaml'
  
  export_add_config(building_config, abel_config, dump_path)
  ```
    
* update_etags
  
  Updates etags in existing onboard config.
  
  Arguments:
  * building_config_path: path to building config export (export a new building config after Onboard-Update operation!)
  * onboard_config_path: path to existing onboard config
 
  ```
  building_config = './building_export.yaml'
  abel_config = './abel_config.yaml'
  
  update_etags(building_config, abel_config)
  ```
    
* update_existing_entities
  
  Used for onboarded active buildings having existing translations that must not be broken.
  
  Takes a new config file, compares it with building config for each Entity: if a reporting field in new Virtual Entity
  already exists in its Reporting Entity translation, the original reporting field name is used in the Virtual Entity instead.
  
  If there are new fields, they are added to existing translation.
  
  Arguments:
  * existing_config: path to building config export
  * new_config: path to ABEL config
 
* export_translations

  Returns pandas dataframe with translation fields for select Entity guids.
  
  Arguments:
  * list of paths to building configs
  * list of entity guids
