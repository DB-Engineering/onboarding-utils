import os
import pandas as pd
import yaml
import sys

from helpers import helpers

pd.options.mode.chained_assignment = None

def load_file(file_path, **kwargs):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    root, ext = os.path.splitext(file_path)

    try:
        if not ext:
            print(f"Not a file: {file_path}")
            return None
        elif ext == ".csv":
            return pd.read_csv(file_path, **kwargs)
            return None
        elif ext == ".xlsx":
            return pd.read_excel(file_path, **kwargs)
        else:
            print(f"Unknown file format: {file_path}")
            return None
    except Exception as e:
        print(f"Could not load file {file_path}: {e}")

def finalize_id(row):
    if row['name_count'] > 1:
        return f"{row['cloud_device_id']}{int(row['suffix'])}"
    return str(row['cloud_device_id'])

def process_mango_config(mango_config: pd.DataFrame):
    print("Processing mango config...")
    try:
        mango_config = mango_config.loc[:, ['pointLocator/configurationDescription', 'tags/proxy_id']].dropna(how='any').drop_duplicates().reset_index(drop=True)
        mango_config['pointLocator/configurationDescription'] = 'device'+mango_config['pointLocator/configurationDescription']
        mango_config.columns = ['device_name', 'cloud_device_id']

        return mango_config
    except Exception as e:
        print(f"Unable to process mango config: {e}")
        sys.exit()

def process_loadsheet(loadsheet: pd.DataFrame, mango_config: pd.DataFrame = None):
    PROXY_MAP = {'OA': 'WST',
                 'HW': 'MTHW',
                 'DHW': 'LTHW',
                 'HHWP': 'HWP',
                 'CWP': 'CDWP',
                 'VAVRH': 'VAV',
                 'VAVCO': 'VAV',
                 'EVAV': 'VAVE',
                 'Hood': 'FHEX'
                }

    print("Processing loadsheet...")
    try:
        loadsheet = loadsheet.loc[(loadsheet['required']=='YES') & (loadsheet['isMissing']!='YES'), :]
        loadsheet.loc[:, 'device_name'] = loadsheet['deviceId'].str.replace('DEV:', 'device')
        loadsheet.loc[:, 'units'] = loadsheet['units'].apply(helpers.snake_to_camel)

        # generate proxy id for each asset

        # simple enumeration by asset and general type
        #loadsheet['proxy_id'] = loadsheet['generalType'] + "-" + loadsheet.groupby('generalType')['assetName'].transform(lambda x: pd.factorize(x)[0] + 1).astype(str)

        # alternative enumeration preserving enumeration from asset name and applying additional suffix for possible duplicates
        loadsheet.loc[:, 'assetName_bdns'] = loadsheet['assetName'].str.split(' ').str.get(0).apply(lambda x: PROXY_MAP.get(x) if PROXY_MAP.get(x) else x)
        loadsheet.loc[:, 'cloud_device_id'] = loadsheet['assetName_bdns'] + "-" + loadsheet['assetName']\
                                                                                                .str.replace('CO2', '')\
                                                                                                .str.replace(r'[^\d]', '', regex=True)\
                                                                                                .replace('', '1')
        
        loadsheet.loc[:, 'name_count'] = loadsheet.groupby('cloud_device_id')['assetName'].transform('nunique')
        mask = loadsheet['name_count'] > 1
        loadsheet.loc[mask, 'enum'] = loadsheet[mask].groupby(['cloud_device_id', 'assetName']).ngroup()
        loadsheet.loc[:, 'suffix'] = loadsheet[mask].groupby('cloud_device_id')['assetName']\
                                                .transform(lambda x: pd.factorize(x)[0] + 1)
        loadsheet.loc[:, 'cloud_device_id'] = loadsheet.apply(finalize_id, axis=1)
        loadsheet = loadsheet.drop(columns=['name_count', 'suffix', 'enum'], errors='ignore')

        # replace cloud_device_id with existing from mango config:
        if mango_config:
            for dev in loadsheet.device_name.unique():
                df_slice = mango_config.loc[mango_config['device_name']==dev, :]
                if df_slice.shape[0] == 0:
                    print(f"No existing proxy_id for {dev}, applying new.")
                elif df_slice.shape[0] > 1:
                    print(f"{dev} contains multiple proxy_id: {', '.join(df_slice.unique().tolist())}, requires manual review. Skipping.")
                else:
                    loadsheet.loc[loadsheet['device_name']==dev, 'cloud_device_id'] = df_slice['cloud_device_id'].values[0]

        loadsheet['object'] = loadsheet['objectType'].map(helpers.OBJECT_ID_MAP_BMS_TO_CAMEL) + ":" + loadsheet['objectId'].astype(str)
        loadsheet['cloud_point_name'] = loadsheet['standardFieldName']
        print("Loadsheet processed successfully.")

        return loadsheet
    except Exception as e:
        print(f"Unable to process loadsheet: {e}")
        sys.exit()

def process_bacnet_scan(bacnet_scan: pd.DataFrame, loadsheet: pd.DataFrame):
    print("Processing bacnet scan...")

    new_bacnet_scan = {}
    unit_validation = pd.DataFrame()
    network_visibility = pd.DataFrame()
    new_bacnet_scan['proxy_id validation'] = loadsheet[['device_name', 'assetName', 'cloud_device_id']].drop_duplicates()
    loadsheet_devices = loadsheet['device_name'].dropna().unique().tolist()

    # Handling objects missing in bacnet-scan
    missing_devices = [d for d in loadsheet_devices if d not in bacnet_scan]
    print("\n [WARNING] Devices not found in bacnet scan, the tabs for these devices will be constructed from loadsheet:\n", ', '.join(missing_devices), "\n")

    for sheet_name, df in bacnet_scan.items():
        if sheet_name == 'devices':
            temp = df[df['device_name'].isin(loadsheet_devices)]
            md = pd.DataFrame({
                'device_name': missing_devices,
                'sanitized_device_name': missing_devices,
                'ip_address': [helpers.device_id_to_ip_addr(d) for d in missing_devices],
                'device_id': [d.replace('device', '') for d in missing_devices]
                })

            temp = pd.concat([df, md], ignore_index=True)
            temp['number'] = temp.index
            new_bacnet_scan['devices'] = temp.copy()

        elif sheet_name in loadsheet_devices:
            df_result = pd.merge(loadsheet.loc[
                                                  loadsheet['device_name']==sheet_name, 
                                                  ['device_name', 'object', 'cloud_device_id', 
                                                  'cloud_point_name', 'units', 'location', 'controlProgram', 'name', 'objectName']
                                              ],
                                 df.drop(columns=['cloud_device_id', 'cloud_point_name'], errors='ignore'), 
                                on=['device_name', 'object'],
                                how='outer',
                                indicator=True)
            df_result.loc[df_result['sanitized_device_name'].isna()==True, 'sanitized_device_name'] = df_result.loc[df_result['sanitized_device_name'].isna()==True, 'device_name']
            df_result.loc[df_result['description'].isna()==True, 'description'] = df_result.loc[df_result['description'].isna()==True, 'name']
            df_result.loc[df_result['point_name'].isna()==True, 'point_name'] = df_result.loc[df_result['point_name'].isna()==True, 'objectName']

            missing_points = df_result.loc[
                                            df_result['_merge'] == 'left_only', 
                                            ['location', 'controlProgram', 'device_name', 'object', 'objectName', 'cloud_point_name']]
            missing_points.columns = ['location', 'controlProgram', 'device_name', 'object', 'objectName', 'standard_field_name']

            network_visibility = pd.concat([network_visibility, missing_points], axis=0)

            mask = df_result['object'].str.contains('analog', na=False) & df_result['cloud_point_name']

            unit_temp = df_result.loc[mask & 
                                     (df_result['cloud_point_name']) &
                                     (df_result['units_or_states']!=df_result['units']) &
                                     (df_result['units_or_states'].isna()==False), 
                                     ['location', 'controlProgram', 'device_name', 'object', 'point_name', 'cloud_point_name', 'units_or_states', 'units']]
            unit_temp.columns = ['location', 'controlProgram', 'device_id', 'object', 'name', 'DBO_fieldname', 'current_units', 'correct_units']

            df_result.loc[mask, 'units_or_states'] = df_result.loc[mask, 'units']
            new_bacnet_scan[sheet_name] = df_result[['point_name', 'device_name', 'sanitized_device_name', 'value', 'units_or_states', 
                                    'description', 'object', 'cloud_value', 'validation_status', 'cloud_device_id', 'cloud_point_name']]

            unit_validation = pd.concat([unit_validation, unit_temp], axis=0)
        else:
            # pass
            print(f"{sheet_name} not in required devices list from loadsheet, skipping.")

    new_bacnet_scan['unit validation'] = unit_validation.drop_duplicates()
    new_bacnet_scan['network_visibility'] = network_visibility.drop_duplicates()

    print("Bacnet scan processed successfully.")

    for dev in missing_devices:
        # recreate tabs for missing devices
        ls = loadsheet.loc[loadsheet['device_name']==dev, ['name', 'object', 'objectName', 'cloud_device_id', 'cloud_point_name', 'units']]
        temp = pd.DataFrame()

        temp["point_name"] = ls['objectName'].copy()
        temp["device_name"], temp["sanitized_device_name"] = dev, dev
        temp["value"] = ''
        temp["units_or_states"] = ls['units'].copy()
        temp["description"] = ls['name'].copy()
        temp["object"] = ls['object'].copy()
        temp["cloud_device_id"] = ls['cloud_device_id']
        temp["cloud_point_name"] = ls['cloud_point_name']
        temp["cloud_value"], temp["validation_status"] = '', ''

        new_bacnet_scan[dev] = temp

    return {
    'proxy_id validation': new_bacnet_scan['proxy_id validation'],
    'unit validation': new_bacnet_scan['unit validation'],
    'network visibility': new_bacnet_scan['network_visibility'],
    'devices': new_bacnet_scan['devices']} | {k: new_bacnet_scan[k] for k in sorted(new_bacnet_scan.keys()) if k not in ['devices', 'proxy_id validation', '']}


def main():
    mango_config_prompt = input("Would you like to load a mango config file? Y/N: ")
    if mango_config_prompt.lower()=='y':
        mango_config_path = input("Insert path to mango config (.csv): ")
        #load mango config
        try:
            mango_config = load_file(mango_config_path, dtype=str)
        except Exception as e:
            print(f"Unable to read mango config: {e}")
            sys.exit()

        mango_config = process_mango_config(process_mango_config)
    else: mango_config = None


    loadsheet_path = input("Insert path to loadsheet (.xlsx): ")
    if not(loadsheet_path):
        raise ValueError("Loadsheet path is required!")

    #load loadsheet
    try:
        loadsheet = load_file(loadsheet_path, dtype=str)
    except Exception as e:
        print(f"Unable to read loadsheet: {e}")
        sys.exit()

    if not mango_config:
        loadsheet = process_loadsheet(loadsheet)
    else:
        loadsheet = process_loadsheet(loadsheet, mango_config)

    bscan_path = input("Insert path to bacnet scan (.xlsx): ")
    if not(bscan_path):
        raise ValueError("Bacnet-scan path is required!")

    # load bacnet scan
    try:
        bacnet_scan = load_file(bscan_path, sheet_name=None, dtype=str)
    except Exception as e:
        print(f"Unable to read bacnet scan: {e}")
        sys.exit()

    bacnet_scan = process_bacnet_scan(bacnet_scan, loadsheet)


    print("Saving results...")
    try:
        output_file_path = bscan_path.replace(".xlsx", "_processed.xlsx")
        with pd.ExcelWriter(output_file_path) as writer:
            for sheet_name, df in bacnet_scan.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"File saved: {output_file_path}")
    except Exception as e:
        print(f"Unable to save file: {e}")
        sys.exit()

if __name__=="__main__":
    main()