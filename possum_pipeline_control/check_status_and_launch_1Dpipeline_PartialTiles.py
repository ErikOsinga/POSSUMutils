import os
from vos import Client
import subprocess
import time
from time import sleep
import re
from automation import database_queries as db
import util

"""
Should be executed on p1

Checks POSSUM Partial Tile status (google sheet)if 1D pipeline can be started.

https://docs.google.com/spreadsheets/d/1_88omfcwplz0dTMnXpCj27x-WSZaSmR-TEsYFmBD43k/edit?usp=sharing

Updates the POSSUM tile status (google sheet) to "running" if 1D pipeline is submitted.


A 1D pipeline run can be started if on the sheet called "Partial Tile Pipeline - [centers/edges] - Band [number]":

1. the "SBID" column is not empty (indicating it has been observed), and
2. the "number_sources" column is not empty (indicating the sourcelist has been created)
3. the "1d_pipeline" column is empty (indicating it hasn't been run yet)


Also checks CANFAR directory
/arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/

for the existence of the input sourcelists.

Cameron is in charge of generating the source lists per partial tile and separately for the centers/edges.

@author: Erik Osinga
"""

def get_results_per_field_sbid_skip_edges(band_number, verbose=False):
    """
    Group the tile_table by 'field_name' and 'sbid' and check the validation condition
    for each group while skipping rows that are considered edge rows.
    
    An edge row is defined as a row where the 'type' column contains
    the substring "crosses projection boundary" (case insensitive).
    
    For the remaining rows within each group, the conditions are met if:
       - all '1d_pipeline_validation' entries are ''
       - all '1d_pipeline' entries are 'Completed'
       
    If after filtering there are no rows remaining in the group, the function returns False for that group.
    
    Args:
        band_number: '1' or '2'
        verbose (bool): If True, print a message for each group.
    
    Returns:
        dict: A dictionary with keys as (field_name, sbid) tuples and boolean values indicating whether 
              the conditions are met for the non-edge rows.
    """
    rows = db.get_observations_non_edge_rows(band_number)
    results = {}

    # Group the table by 'field_name' and 'sbid'
    for row in rows:
        results[(row[0], row[1])] = row[2]
        if verbose:
            status = "all conditions met" if row[2] is True else "conditions not met"
            print(f"Field '{row[0]}', SBID '{row[1]}': after filtering edges, conditions are {status}.")

    return results

def get_results_per_field_sbid(band_number='1', verbose=False):
    """
    Group observation by field name and sbid

    Returns a dict of type {('fieldname','sbid'):boolean}

    If all Partial tiles for a fieldname have been completed boolean=True, otherwise false.
    """
    # Group the table by 'field_name' and 'sbid'
    results = db.get_observations_with_complete_partial_tiles(band_number)
    field_sbid_dict = {}
    for row in results:
        field_sbid_dict[(row[0], row[1])] = row[2]
        if verbose:
            status = "all conditions met" if row[2] is True else "conditions not met"
            print(f"""Field '{row[0]}', SBID '{row[1]}' has 1d_pipeline_validation=''
                  and 1d_pipeline='{status}'.""")
    return field_sbid_dict

def remove_prefix(field_name):
    """
    Remove the prefix "EMU_" or "WALLABY_" from the field name.
    
    e.g. 
    s = tile_table['field_name'][1935]
    print(s) # output will be 'EMU_2108-09A'
    s = remove_prefix(s)
    print(s)  # Output will be '2108-09A'
    """
    # The regex looks for either "EMU_" or "WALLABY_" at the beginning of the string
    return re.sub(r'^(EMU_|WALLABY_)', '', field_name)

def get_tiles_for_pipeline_run(band_number):
    """
    Get a list of tile numbers that should be ready to be processed by the 1D pipeline 
    
    i.e.  'SBID' column is not empty, 'number_sources' is not empty, and '1d_pipeline' column is empty
    
    Args:
    band_number (int): The band number (1 or 2) to check.

    Returns:
    list: A list of tile numbers that satisfy the conditions.
    """
    # Find the tiles that satisfy the conditions
    # (i.e. has an SBID and not yet a '1d_pipeline' status)
    rows = db.get_partial_tiles_for_1d_pipeline_run(band_number)
    fields_to_run, SBids_to_run = [],[]
    tile1_to_run, tile2_to_run, tile3_to_run, tile4_to_run = [],[],[],[]
    for row in rows:
        fields_to_run.append(remove_prefix(row[0]))
        SBids_to_run.append(row[1])
        tile1_to_run.append(row[2])
        tile2_to_run.append(row[3])
        tile3_to_run.append(row[4])
        tile4_to_run.append(row[5])

    # Also find fields that have all partial tiles completed, but validation plot not yet made
    results_grouped = get_results_per_field_sbid(band_number)
    # return only a list of (fieldnames,sbid) pairs for which we can start a validation job
    can_make_validation = [key for (key,value) in results_grouped.items() if value]

    # Also find fields that have all partial tiles completed except edge cases, and validation plot not yet made
    results_grouped_plus_edges = get_results_per_field_sbid_skip_edges(band_number)
    can_make_val_if_skip_edges = [ key for (key,value) in results_grouped_plus_edges.items() if value]
    # remove the ones from the edges list if already in the full list
    # so this list contains only the projection boundary cases
    can_make_val_if_skip_edges = set(can_make_val_if_skip_edges) - set(can_make_validation)

    return fields_to_run, tile1_to_run, tile2_to_run, tile3_to_run, tile4_to_run, SBids_to_run, can_make_validation, can_make_val_if_skip_edges

def get_canfar_sourcelists(band_number, local_file="./sourcelist_canfar.txt"):
    client = Client()
    # force=True to not use cache
    # assumes directory structure doesnt change and symlinks are created
    print("Getting sourcelists from CANFAR...")
    
    # Check if cache file exists and is less than a day old
    if os.path.exists(local_file):
        file_mod_time = os.path.getmtime(local_file)
        if (time.time() - file_mod_time) < 86400:  # 86400 seconds = 1 day
            print(f"Reading sourcelists from local file cache: {local_file}")
            with open(local_file, "r") as f:
                canfar_sourcelists = f.read().splitlines()
            return canfar_sourcelists

    if band_number == 1:
        # canfar_sourcelists = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/",force=True)
        # disabled above command due to issue with client.listdir for many files https://github.com/opencadc/vostools/issues/228
        
        cmd = "vls vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/"
        result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
        if result.returncode == 0:
            output = result.stdout
            canfar_sourcelists = output.splitlines()
        else:
            print("Error running command. Perhaps CANFAR is down?")
            raise ValueError(f"Error running command: {cmd}\n{result.stderr}")

    elif band_number == 2:
        # TODO
        raise NotImplementedError("TODO")
        canfar_sourcelists = client.listdir("vos://cadc.nrc.ca~arc/projects/CIRADA/polarimetry/ASKAP/Tiles/1367MHz/",force=True)
    else:
        raise ValueError(f"Band number {band_number} not defined")
    
    # Save the results to the local cache file for future use
    with open(local_file, "w") as f:
        f.write("\n".join(canfar_sourcelists))
    
    return canfar_sourcelists

def field_from_sourcelist_string(srclist_str):
    """
    Extract field ID from sourcelist string

    e.g. 

    selavy-image.i.EMU_1441+04B.SB59835.cont.taylor.0.restored.conv.components.xml

    returns 

    1441+04B
    """
    field_ID = srclist_str.split(".")[2]  # Extract the field ID from the sourcelist string
    # Remove the prefix "EMU_" or "WALLABY_"
    field_ID = remove_prefix(field_ID)

    if "EMU" in srclist_str:
        pass
    elif "WALLABY" in srclist_str:
        pass
    else:
        field_ID = None
        print(f"Warning, could not find field_ID for sourcelist {srclist_str}")
    
    return field_ID

def launch_pipeline(field_ID, tilenumbers, SBid, band):
    """
    # Launch the appropriate 1D pipeline script based on the band

    field_ID    -- str/int         -- 7 char fieldID, e.g. 1412-28
    tilenumbers -- list of str/int -- list of up to 4 tile numbers: a tile number is a 4 or 5 digit tilenumber, e.g. 8972, if no number it's an empty string '' 
    SBid        -- str/int         -- 5 (?) digit SBid, e.g. 50413
    band        -- str             -- '943MHz' or '1367MHz' for Band 1 or Band 2

    """
    if band == "943MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band1.py", str(field_ID), str(tilenumbers), str(SBid)]
    elif band == "1367MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band2.py", str(field_ID), str(tilenumbers), str(SBid)]
        command = ""
        raise NotImplementedError("TODO: Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)

def launch_pipeline_summary(field_ID, SBid, band):
    """
    # Launch the appropriate 1D pipeline summary script based on the band

    field_ID    -- str/int         -- 7 char fieldID, e.g. 1412-28
    SBid        -- str/int         -- 5 (?) digit SBid, e.g. 50413
    band        -- str             -- '943MHz' or '1367MHz' for Band 1 or Band 2

    """
    if band == "943MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band1_pre_or_post.py", str(field_ID), str(SBid), "post"]
    elif band == "1367MHz":
        command = ["python", "launch_1Dpipeline_PartialTiles_band2_pre_or_post.py", str(field_ID), str(SBid), "post"]
        command = ""
        raise NotImplementedError("TODO: Temporarily disabled launching band 2 because need to write that run script")
    else:
        raise ValueError(f"Unknown band: {band}")

    print(f"Running command: {' '.join(command)}")
    subprocess.run(command, check=True)    

def launch_band1_1Dpipeline():
    """
    Launch a headless job to CANFAR for a 1D pipeline Partial Tile
    """
    band = "943MHz"

    # Check google sheet for band 1 tiles that have been ingested into CADC
    # (and thus available on CANFAR) but not yet processed with 3D pipeline
    field_IDs, tile1, tile2, tile3, tile4, SBids, fields_to_validate, field_to_validate_boundaryissues = get_tiles_for_pipeline_run(band_number=1)
    assert len(tile1) == len(tile2) == len(tile3) == len(tile4), "Need to have 4 tile columns in google sheet. Even if row can be empty."
    # list of full sourcelist filenames
    canfar_sourcelists = get_canfar_sourcelists(band_number=1)
    # canfar_sourcelists = ['selavy-image.i.EMU_0314-46.SB59159.cont.taylor.0.restored.conv.components.15sig.xml',
    #                       'selavy-image.i.EMU_0052-37.SB46971.cont.taylor.0.restored.conv.components.15sig.xml',
    #                       'selavy-image.i.EMU_1227-69.SB61103.cont.taylor.0.restored.conv.components.15sig.xml',
    #                       ]
    # list of only the field IDs e.g. "1428-12"
    sourcelist_fieldIDs = [field_from_sourcelist_string(srl) for srl in canfar_sourcelists]
    sleep(1) # google sheet shenanigans

    assert len(field_IDs) == len(tile1) == len(SBids) # need fieldID, up to 4 tileIDs and SBID to define what to run

    band_number = util.get_band_number(band)
    # First launch ALL 1D pipeline summary plot jobs if any fields are fully finished
    if len(fields_to_validate) > 0:
        print(f"Found {len(fields_to_validate)} fields that are fully finshed: {fields_to_validate}\n")

        for (field, sbid) in fields_to_validate:
            print(f"Launching job to create summary plot for field {field} with sbid {sbid}")
            
            field = remove_prefix(field)

            launch_pipeline_summary(field, sbid, band)

            # Update the status of the '1d_pipeline_validation' column to "Running" regardless of tile number
            db.update_1d_pipeline_validation_status(field, sbid, band_number, "Running")

    if len(field_to_validate_boundaryissues) > 0:
        print(f"Found {len(field_to_validate_boundaryissues)} fields that are partially finished (except projection boundaries): {field_to_validate_boundaryissues}\n")

        for (field, sbid) in field_to_validate_boundaryissues:
            print(f"Launching job to create summary plot for field {field} with sbid {sbid} (skipping edges)")
            
            field = remove_prefix(field)

            launch_pipeline_summary(field, sbid, band)

            # Update the status of the '1d_pipeline_validation' column to "Running" regardless of tile number
            db.update_1d_pipeline_validation_status(field, sbid, band_number, "Running")


    if len(field_IDs) > 0:
        print(f"Found {len(field_IDs)} partial tile runs in Band 1 ready to be processed with 1D pipeline")
        print(f"On CANFAR, found {len(sourcelist_fieldIDs)} sourcelists for Band 1")

        # if len(field_IDs) > len(sourcelist_fieldIDs):
        tiles_ready_but_not_canfar = set(field_IDs) - set(sourcelist_fieldIDs)
        print(f"Field IDs ready according to the sheet but sourcelist not on CANFAR: {tiles_ready_but_not_canfar}")
        # else:
        tiles_canfar_not_ready = set(sourcelist_fieldIDs) - set(field_IDs)
        # print(f"Field ID sourcelists on CANFAR but not ready to run: {tiles_canfar_not_ready}")

        fields_on_both = set(field_IDs) & set(sourcelist_fieldIDs)
        # print(f"Fields ready on both CADC and CANFAR: {tiles_on_both}")
        print(f"Number of fields ready according to the sheet and available on CANFAR: {len(fields_on_both)}")

        if fields_on_both:
            # Launch the first field_ID that has a sourcelist (assumes this script will be called many times)
            for i in range(len(field_IDs)):
                field_ID = field_IDs[i]
                t1 = tile1[i]
                t2 = tile2[i]
                t3 = tile3[i]
                t4 = tile4[i]
                tilenumbers = [t1, t2, t3, t4] # up to four tilenumbers, or less with '' (empty strings)
                SBid = SBids[i]
                if field_ID not in fields_on_both:
                    print(f"Skipping {field_ID} as it doesnt have a sourcelist on CANFAR")
                    continue
                else:
                    print(f"\nLaunching headless job for 1D pipeline for field {field_ID} observed in SBid {SBid} covering partial tiles {tilenumbers} ")

                    # Launch the pipeline
                    launch_pipeline(field_ID, tilenumbers, SBid, band)
                    
                    # Update the status to "Running"
                    db.update_partial_tile_1d_pipeline_status(field_ID, tilenumbers, band_number, "Running")

                    break
            
        else:
            print("No tiles are available on both CADC and CANFAR.")
    else:
        print("Found no tiles ready to be processed.")

if __name__ == "__main__":
    launch_band1_1Dpipeline()
