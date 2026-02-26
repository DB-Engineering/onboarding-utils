import json
import os
import pandas as pd
import yaml
import uuid
import openpyxl
import sys

object_id_map = {
    "AV": "analogValue",
    "AI": "analogInput",
    "AO": "analogOutput",
    "BV": "binaryValue",
    "BI": "binaryInput",
    "BO": "binaryOutput",
    "MSV": "multiStateValue"
}

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

def to_camel(x: str):
    if not isinstance(x, str):
        return None
    parts = x.split('-')
    return parts[0] + ''.join(p.capitalize() for p in parts[1:])



if __name__=="__main__":

    loadsheet_path = input("Insert path to loadsheet (.xlsx): ")
    #load loadsheet
    try:
        loadsheet = load_file(loadsheet_path, dtype=str)
    except Exception as e:
        print(f"Unable to read loadsheet: {e}")
        sys.exit()

    bscan_path = input("Insert path to bacnet scan (.xlsx): ")
    # load bacnet scan
    try:
        bacnet_scan = load_file(bscan_path, sheet_name=None, dtype=str)
    except Exception as e:
        print(f"Unable to read bacnet scan: {e}")
        sys.exit()

    mango_config = None
    mango_config_prompt = ("Would you like to load a mango config file? Y/N: ")
    if mango_config_prompt.lower()=='y':
        mango_config_path = input("Insert path to mango config (.csv): ")
        #load mango config
        try:
            mango_config = load_file(mango_config_path, dtype=str)
        except Exception as e:
            print(f"Unable to read mango config: {e}")
            sys.exit()

        print("Processing mango config...")
        try:
            mango_config = mango_config.loc[:, ['pointLocator/configurationDescription', 'tags/proxy_id']].dropna(how='any').drop_duplicates().reset_index(drop=True)
            mango_config['pointLocator/configurationDescription'] = 'device'+mango_config['pointLocator/configurationDescription']
            mango_config.columns = ['device_name', 'cloud_device_id']
        except Exception as e:
            print(f"Unable to process mango config: {e}")
            sys.exit()

    print("Processing loadsheet...")
    try:
        loadsheet = loadsheet.loc[(loadsheet['required']=='YES') & (loadsheet['isMissing']!='YES'), :]
        loadsheet['device_name'] = loadsheet['deviceId'].str.replace('DEV:', 'device')
        loadsheet['units'] = loadsheet['units'].apply(to_camel)

        # generate proxy id for each asset

        # simple enumeration by asset and general type
        #loadsheet['proxy_id'] = loadsheet['generalType'] + "-" + loadsheet.groupby('generalType')['assetName'].transform(lambda x: pd.factorize(x)[0] + 1).astype(str)

        # alternative enumeration preserving enumeration from asset name and applying additional suffix for possible duplicates
        loadsheet['cloud_device_id'] = loadsheet['generalType'] + "-" + loadsheet['assetName']\
                                                                        .str.replace('CO2', '')\
                                                                        .str.replace(r'[^\d]', '', regex=True)\
                                                                        .replace('', '1')
        loadsheet['name_count'] = loadsheet.groupby('cloud_device_id')['assetName'].transform('nunique')
        mask = loadsheet['name_count'] > 1
        loadsheet.loc[mask, 'enum'] = loadsheet[mask].groupby(['cloud_device_id', 'assetName']).ngroup()
        loadsheet['suffix'] = loadsheet[mask].groupby('cloud_device_id')['assetName']\
                                                .transform(lambda x: pd.factorize(x)[0] + 1)
        loadsheet['cloud_device_id'] = loadsheet.apply(finalize_id, axis=1)
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

        loadsheet['object'] = loadsheet['objectType'].map(object_id_map) + ":" + loadsheet['objectId'].astype(str)
        loadsheet['cloud_point_name'] = loadsheet['standardFieldName']
        loadsheet_devices = loadsheet['device_name'].dropna().unique().tolist()
        print("Loadsheet processed successfully.")
    except Exception as e:
        print(f"Unable to process loadsheet: {e}")
        sys.exit()

        print("Processing bacnet scan...")
        new_bacnet_scan = {}
        unit_validation = pd.DataFrame()
        new_bacnet_scan['proxy_id validation'] = loadsheet[['device_name', 'assetName', 'cloud_device_id']].drop_duplicates()
        new_bacnet_scan['unit validation']= None

        for sheet_name, df in bacnet_scan.items():
            if sheet_name == 'devices':
                new_bacnet_scan[sheet_name] = df[df['device_name'].isin(loadsheet_devices)] # should reset enumeration for "number"??
            elif sheet_name in loadsheet_devices:
                df_result = pd.merge(df.drop(columns=['cloud_device_id', 'cloud_point_name'], errors='ignore'), 
                                    loadsheet.loc[loadsheet['device_name']==sheet_name, 
                                                  ['device_name', 'object', 'cloud_device_id', 'cloud_point_name', 'units', 'controlProgram']],
                                    on=['device_name', 'object'],
                                    how='left')
                mask = df_result['object'].str.contains('analog', na=False) & df_result['cloud_point_name']

                unit_temp = df_result.loc[mask & 
                                         (df_result['cloud_point_name']) &
                                         (df_result['units_or_states']!=df_result['units']), 
                                                    ['controlProgram', 'device_name', 'object', 'point_name', 'units_or_states', 'units']]
                unit_temp.columns = ['controlProgram', 'device_id', 'object', 'name', 'current_units', 'correct_units']

                df_result.loc[mask, 'units_or_states'] = df_result.loc[mask, 'units']
                new_bacnet_scan[sheet_name] = df_result.drop(['units', 'controlProgram'], axis=1)

                unit_validation = pd.concat([unit_validation, unit_temp], axis=0)
            else:
                # pass
                print(f"{sheet_name} not in required devices list from loadsheet, skipping.")
        new_bacnet_scan['unit validation'] = unit_validation
        print("Bacnet scan processed successfully.")
    except Exception as e:
        print(f"Unable to process bacnet scan: {e}")
        sys.exit()

    print("Saving results...")
    try:
        output_file_path = bscan_path.replace(".xlsx", "_processed.xlsx")
        with pd.ExcelWriter(output_file_path) as writer:
            for sheet_name, df in new_bacnet_scan.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"File saved: {output_file_path}")
    except Exception as e:
        print(f"Unable to save file: {e}")
        sys.exit()