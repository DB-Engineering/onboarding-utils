import numpy as np
import pandas as pd
# import ruamel.yaml as yaml
from ruamel.yaml import YAML

yaml = YAML(typ='rt')

def export_update_config(building_config_path, abel_config_path, dump_path, entity_list = None, max_items = 50):
    '''
    Exports Onboard-Update config yaml. If there are more than 100 Entities to update, multiple config files will be exported to prevent DB API operation deadline errors.
    Args:
        building_config_path: path to building config export
        abel_config_path: path to ABEL config
        dump_path: path to the new onboard-update yaml
        entity_list: list of Entity Guids, if only need to export a config for select Entities. Default: None, all entities will be exported.
    '''
    MAX_ITEMS_PER_CONFIG = max_items

    with open(building_config_path, 'r') as f:
        building_config = yaml.load(f)
        f.close()
    with open(abel_config_path, 'r') as f:
        abel_config = yaml.load(f)
        f.close()

    new_abel_config = {}
    if entity_list:
        for key, val in abel_config.items():
            if key in entity_list:
                new_abel_config[key] = val
        abel_config = new_abel_config.copy()

    config_top = {}
    config_top['CONFIG_METADATA'] = {'operation': "UPDATE"}
    building = [{key: val} for key, val in building_config.items() if val.get('type') and val['type']=='FACILITIES/BUILDING'][0]
    config_top = config_top | building

    num_reporting_entities = len([key for key, val in abel_config.items() if 'translation' in val])
    print(f'{num_reporting_entities} reporting entities found')

    update_config = {}
    entity_counter = 0
    chunk_counter = 1

    for idx, items in enumerate(abel_config.items()):
        key = items[0]
        val = items[1]

        if val.get('translation'):
            if val.get('operation'):
                val.pop('operation')
            else: pass

            if key in building_config.keys():
                if building_config[key].get('etag'):
                    etag = str(building_config[key].get('etag'))
                else: etag = 'MISSING ETAG'
                update_config[key] = {'operation': 'update'} \
                                | {'etag': etag} \
                                | val \
                                | {'update_mask': ['type', 'translation']}
                print('Added ', key)
                entity_counter += 1
            else: print(f'Not in building config: {key}')
        else: pass

        if entity_counter!=0 and any([entity_counter%MAX_ITEMS_PER_CONFIG==0, idx==len(abel_config.keys())-1]):
            print(f"Saving chunk {chunk_counter}, {entity_counter} entities.\n")
            new_file_name = dump_path.replace('.yaml', f'_pt{chunk_counter}.yaml')
            update_config = config_top | update_config

            with open(new_file_name, 'w') as f:
                for key, value in update_config.items():
                    yaml.dump({key: value}, f)
                    f.write('\n')
                f.close()

            update_config = {}
            chunk_counter += 1
            entity_counter = 0
        else: pass


def export_add_config(building_config_path, abel_config_path, dump_path):
    '''
    Exports Onboard-Add config yaml.
    Args:
        building_config_path: path to building config export (export a new building config after Onboard-Update operation!)
        abel_config_path: path to ABEL config
        dump_path: path to the new onboard-add yaml
    '''
    with open(building_config_path, 'r') as f:
        building_config = yaml.load(f)
        f.close()
    with open(abel_config_path, 'r') as f:
        abel_config = yaml.load(f)
        f.close()

    config_top = {}
    config_top['CONFIG_METADATA'] = {'operation': "UPDATE"}
    building = [{key: val} for key, val in building_config.items() if val.get('type') and val['type']=='FACILITIES/BUILDING'][0]
    config_top = config_top | building

    reporting = {}
    for key, val in abel_config.items():
        if 'update_mask' in val.keys():
            val.pop('update_mask')
        if 'operation' in val.keys():
            val.pop('operation')
        if val.get('translation'):
            if key in building_config.keys():
                etag = str(building_config[key].get('etag'))
            else: 
                etag = 'missing_etag'
                print(f'Not in building config: {key}')

            reporting[key] = {'etag': etag} \
                            | val

    virtual = {}
    for key, val in abel_config.items():
        if val.get('links'):
            virtual[key] = {'operation': 'add'} | val
    add_config = config_top | reporting | virtual
    with open(dump_path, 'w') as f:
        for key, value in add_config.items():
            yaml.dump({key: value}, f)
            f.write('\n')
        f.close()


def update_etags(building_config_path, onboard_config_path):
    '''
    Updates etags in existing onboard config.
    Args:
        building_config_path: path to building config export (export a new building config after Onboard-Update operation!)
        onboard_config_path: path to existing onboard config
    '''
    with open(building_config_path) as f:
        building_conf = yaml.load(f)
        f.close()
    with open(onboard_config_path) as f:
        onboard_conf = yaml.load(f)
        f.close()

    for guid, val in onboard_conf.items():
        if guid != 'CONFIG_METADATA' and any([val.get('operation') and val.get('operation').lower()=='update',
                                              val.get('translation')]):
            if building_conf[guid].get('etag'):
                onboard_conf[guid] = onboard_conf[guid] | {'etag': building_conf[guid]['etag']}
            else: print('No etag for: ', guid, val.get('code'))

    with open(onboard_config_path.replace('.yaml','_upd.yaml'), 'w') as f:
        for key, value in onboard_conf.items():
            yaml.dump({key: value}, f)
            f.write('\n')
        f.close()

def update_existing_entities(existing_config, new_config, dump_path):
    '''
    Used for onboarded active buildings having existing translations that must not be broken.
    Takes a new config file, compares it with building config for each Entity: if a reporting field in new Virtual Entity
    already exists in its Reporting Entity translation, the original reporting field name is used in the Virtual Entity instead.
    If there are new fields, they are added to existing translation.
    Args:
        existing_config: path to building config export
        new_config: path to ABEL config
    '''
    with open(existing_config) as f:
        existing = yaml.load(f)
        f.close()
    with open(new_config) as f:
        new = yaml.load(f)
        f.close()
        
    reporting_update_path = dump_path.replace('.yaml', '_reporting.yaml')
    virtual_update_path = dump_path.replace('.yaml', '_virtual.yaml')
        
    reporting = [key for key in new.keys() if new[key].get('translation') and existing.get(key)]
    update_virtual = [key for key in new.keys() if new[key].get('links') and existing.get(key)]
    add_virtual = [key for key in new.keys() if new[key].get('links') and not existing.get(key)]
    
    print('Reporting Entities found:')
    for i in reporting:
        print(i)
    if len(update_virtual) > 0:
        print("Existing Virtual Entities found:")
        for i in update_virtual:
            print(i)
    if len(add_virtual) > 0:
        print("New Virtual Entities found:")
        for i in add_virtual:
            print(i)

    # creating configs
    update_config_reporting = {}
    update_config_reporting['CONFIG_METADATA'] = {'operation': 'UPDATE'}
    building = [key for key in existing.keys() if existing[key].get('type')=='FACILITIES/BUILDING'][0]
    update_config_reporting = update_config_reporting | {building: existing[building]}
    update_config_virtual = update_config_reporting.copy()
    
    #adding reporting entities
    for re in reporting:
#         print(f'\nReporting: {re}')
        # if existing entity doesn't have a translation yet, append a full new entity:
        if not existing[re].get('translation'):
#             print(f'Added to reporting config: {re}')
            update_config_reporting[re] = new[re].copy()
            # adding flags for reporting config
            update_config_reporting[re]['operation'] = 'update'
            update_config_reporting[re]['update_mask'] = ['type', 'translation']
            update_config_reporting[re]['etag'] = existing[re]['etag']
            
#             print(f'Added to virtual config: {re}')
            update_config_virtual[re] = new[re].copy()
        # if existing entity already has a translation, use existing reporting entity fields instead of new ones
        else:
            reporting_entity = existing[re].copy()

            existing_translation_fields = {}
            existing_missing_fields = []

            new_translation_fields = {}
            new_missing_fields = {}

            for key, val in existing[re]['translation'].items():
                if isinstance(val, str): 
                    existing_missing_fields.append(key)
                else:
                    existing_translation_fields[key] = val.get('present_value')

            for key, val in new[re]['translation'].items():
                if isinstance(val, str): 
                    if key not in existing[re]['translation'].keys():
                        new_missing_fields[key] = val
                elif val.get('present_value') not in existing_translation_fields.values():
                    new_translation_fields[key] = val
            print('Added new MISSING fields:')
            for key in new_missing_fields.keys():
                print('   ', key)
            print('Added new translation fields: ')
            for key in new_translation_fields.keys():
                print('   ', key)
            merged_translation = existing[re]['translation'] | new_translation_fields | new_missing_fields

            reporting_entity['translation'] = merged_translation.copy()
            
            # no flag for reporting entities in virtual config
#             print(f'Added to virtual config: {re}')
            update_config_virtual[re] = reporting_entity.copy()
            
#             print(f'Added to reporting config: {re}')
            update_config_reporting[re] = reporting_entity.copy()
            # adding flags for reporting config
            update_config_reporting[re]['operation'] = 'update'
            update_config_reporting[re]['update_mask'] = ['translation']
            update_config_reporting[re]['etag'] = existing[re]['etag']
            
    with open(reporting_update_path, 'w') as f:
        for key, value in update_config_virtual.items():
            yaml.dump({key: value}, f)
            f.write('\n')
        f.close()

    # create an ADD config if virtual entities don't exist
    if len(add_virtual) > 0:
        # copy whole top of virtual config and turn it into ADD
        add_config_virtual = update_config_virtual.copy()
        add_config_virtual['CONFIG_METADATA']['operation'] = 'ADD'
        # add all new virtual entities
        for ve in add_virtual:
            add_config_virtual[ve] = new[ve].copy()
            add_config_virtual[ve]['operation'] = 'add'
            
        for key, val in add_config_virtual.items():
            if val.get('translation'):
                val['type'] = 'GATEWAYS/PASSTHROUGH'
            
        with open(virtual_update_path.replace('update', 'add'), 'w') as f:
            for key, value in add_config_virtual.items():
                yaml.dump({key: value}, f)
                f.write('\n')
            f.close()
            
    if len(update_virtual) > 0:
        # adding virtual entities
        for ve in update_virtual:
            virtual_entity = new[ve].copy()

            # iterate thtough fields in existing link and replace all reporting firlds with existing ones
            for re, fields in existing[ve]['links'].items():
                for st_field, re_field in fields.items():
                    # if new translation has the field
                    if st_field in virtual_entity['links'][re].keys():
                        new_re_field = new[ve]['links'][re][st_field]
                        ex_re_field = existing[ve]['links'][re][st_field]

#                         print('\n--->',new_re_field, ex_re_field)
#                         print('Are reporting fields the same? ', new[ve]['links'][re].get(st_field) == existing[ve]['links'][re].get(st_field))
#                         print(new[re]['translation'][new_re_field].get('present_value'), ' / ', existing[re]['translation'][ex_re_field].get('present_value'))
#                         print('Are linked points the same?: ', new[re]['translation'][new_re_field].get('present_value') == existing[re]['translation'][ex_re_field].get('present_value'))
#                         print('Are repoting fields missing?: ', new[re]['translation'][new_re_field]=='MISSING',' / ',existing[re]['translation'][ex_re_field]=='MISSING')

                        # if both same field in new and existing configs are linked to MISSING reporting field
                        if all([new[re]['translation'].get(new_re_field) == 'MISSING',
                                existing[re]['translation'].get(ex_re_field) == 'MISSING']):
                            # replace new rep field name with existing rep field name
                            n = new[ve]['links'][re][st_field]
                            e = existing[ve]['links'][re][st_field]
                            print(f'\nReplacing {st_field}:\n{n} ---> {e}')
                            virtual_entity['links'][re][st_field] = existing[ve]['links'][re][st_field]
                            
                        # if the same field in new and existing configs are linked to the same point id
                        elif all([new[re]['translation'][new_re_field].get('present_value') == existing[re]['translation'][ex_re_field].get('present_value'),
                                new_re_field != ex_re_field]):
                            # replace new rep field name with existing rep field name
                            n = new[ve]['links'][re][st_field]
                            e = existing[ve]['links'][re][st_field]
                            print(f'\nReplacing {st_field}:\n{n} ---> {e}')
                            virtual_entity['links'][re][st_field] = existing[ve]['links'][re][st_field]

            virtual_entity['operation'] = 'update'
            virtual_entity['etag'] = existing[ve].get('etag')
            virtual_entity['update_mask'] = ['type, links']
            update_config_virtual[ve] = virtual_entity.copy()
#             print(f'Added to virtual config: {ve}')
            
        with open(virtual_update_path, 'w') as f:
            for key, value in update_config_virtual.items():
                yaml.dump({key: value}, f)
                f.write('\n')
            f.close()
     
        # FINAL CHECK
        with open(virtual_update_path, 'r') as f:
            final_config = yaml.load(f)
            f.close()
        translation_check = {
            'virtual_entity_guid': [],
            'reporting_entity_guid': [],
            'stardard_field_name': [],
            'raw_field_name': []
        }

        for ve in update_virtual:
            for re in final_config[ve]['links'].keys():
                for st_field, re_field in final_config[ve]['links'][re].items():
                    raw_field_name = 'MISSING' if isinstance(final_config[re]['translation'].get(re_field), str) else final_config[re]['translation'][re_field].get('present_value')
                    translation_check['virtual_entity_guid'].append(ve)
                    translation_check['reporting_entity_guid'].append(re)
                    translation_check['stardard_field_name'].append(st_field)
                    translation_check['raw_field_name'].append(raw_field_name)

        pd.DataFrame(translation_check).to_csv(dump_path.replace('.yaml', '_translation_check.csv'), index=False)

def export_translations(paths, entities):
    """
    Returns pandas dataframe with translation fields for select guids.
    Args: list of paths to configs, list of entity guids
    """
    for path in paths:
        print(f'Reading {path}')
        with open(path, 'r') as f:
            config = yaml.load(f)
            f.close()
        for ve in entities:
            if ve in config.keys():
                print(f'   {ve}')
                if config[ve].get('links'):
                    for re in config[ve]['links'].keys():
                        for st_field, re_field in config[ve]['links'][re].items():
                            raw_field_name = ('MISSING' if isinstance(config[re]['translation'].get(re_field), str) 
                                                  else config[re]['translation'][re_field].get('present_value'))
                            translation['building'].append(config[ve].get('code').split(':')[0])
                            translation['virtual_entity_guid'].append(ve)
                            translation['virtual_entity_code'].append(config[ve].get('code'))
                            translation['reporting_entity_guid'].append(re)
                            translation['stardard_field_name'].append(st_field)
                            translation['reporting_field_name'].append(re_field)
                            translation['raw_field_name'].append(raw_field_name)
                elif config[ve].get('translation'):
                    for key, val in config[ve]['translation'].items():
                            raw_field_name = ('MISSING' if isinstance(val, str) 
                                                  else val.get('present_value'))
                            translation['building'].append(config[ve].get('code').split(':')[0])
                            translation['virtual_entity_guid'].append(ve)
                            translation['virtual_entity_code'].append(config[ve].get('code'))
                            translation['reporting_entity_guid'].append(None)
                            translation['stardard_field_name'].append(key)
                            translation['reporting_field_name'].append(None)
                            translation['raw_field_name'].append(raw_field_name)
    return pd.DataFrame(translation)