#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updates POSSUM tile status if CADC ingest has completed.

Scrapes the CADC POSSUM collection for file names, compares against the list
on the POSSUMm status monitor, and updates any missing if all the required
products (all 3 cubes, plus the Stokes I MFS image) are present.

Requires CADC authentication, uses the default authentication file (~/.ssl/cadcproxy.pem)
produced by getCert. Also requires the Google sheet API key to write to the
status sheet.

@author: cvaneck
"""


import gspread
import astroquery.cadc as cadc
import astropy.table as at
import numpy as np
import datetime
from time import sleep



def update_sheet_CADC_tiles(CADC_cert_file,Google_API_token):
    
    #Authenticate and grab tables
    session = cadc.Cadc()
    session.login(certificate_file=CADC_cert_file)
    gc = gspread.service_account(filename=Google_API_token)
    ps= gc.open_by_url('https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0')

    #Update both bands:
    update_one_band('1',session,ps)
    update_one_band('2',session,ps)




def update_one_band(band_number,CADC_session,ps):
    """Check for new fully-complete  
    """
    query=CADC_session.create_async("""SELECT observationID,Plane.productID,Observation.lastModified FROM caom2.Plane AS Plane 
	JOIN caom2.Observation AS Observation ON Plane.obsID = Observation.obsID 
    WHERE  (Observation.collection = 'POSSUM') AND (observationID NOT LIKE '%pilot1') """)
    query.run().wait()  
    query.raise_if_error()
    result=query.fetch_result().to_table()
    result.add_column([x.split('_')[-2] for x in result['observationID']], name='tile_number')
    
    freq = '943' if (str(band_number) == '1') else '1367'
    tile_numbers = np.unique([x.split('_')[-2] for x in result['observationID'] if x.startswith(freq)])
    #tile numbers kept as strings for now


    #Get already-updated tile list from status sheet

    tile_sheet = ps.worksheet(f'Survey Tiles - Band {band_number}')
    tile_data=tile_sheet.get_values()
    column_names = tile_data[0]
    tile_table=at.Table(np.array(tile_data)[1:],names=column_names)
    sleep(1)

    col_letter = gspread.utils.rowcol_to_a1(1,column_names.index('aus_src')+1)[0]


    for tile in tile_numbers:
        if tile_table[tile_table['tile_id'] == tile]['aus_src'] != '':
            continue  #this tile already done; ignore
        #If not updated, check for all files/products before updating sheet.
        tile_products=result[result['tile_number'] == tile]
        full=True
        if ((tile_products['productID'] == 'raw_qu').sum() < 1):
            print(f'Band 1 tile {tile} missing QU cubes.')
            full=False
        if ((tile_products['productID'] == 'raw_i').sum() < 1):
            print(f'Band 1 tile {tile} missing I cube.')
            full=False
        if ((tile_products['productID'] == 'multifrequencysynthesis_i_t0').sum() < 1):
            print(f'Band 1 tile {tile} missing MFS I image.')
            full=False
        if not full:
            continue
        else:
            print(f"Band {band_number} {tile} is now complete.")
            #update sheet
            w = np.where(tile_table['tile_id'] == tile)[0]
            if w.size != 1:
                raise Exception(f'Could not uniquely find tile row for {tile}. Either missing or duplicated?')        
            dt=[datetime.datetime.fromisoformat(x) for x in tile_products['lastModified']]
            last_modified=max(dt)
            
            # as of >v6.0.0 .update requires a list of lists
            tile_sheet.update(range_name=f'{col_letter}{int(w[0])+2}', values=[[last_modified.date().isoformat()]]) 
            sleep(1)


if __name__ == "__main__":
    import argparse
    
    descStr = """
    Update the status of tiles ingested into CADC archive (frequency cubes + MFS
    images). User must supply a valid Google Sheets API key and CADC cert file.
    """

    parser = argparse.ArgumentParser(description=descStr,
                                 formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("Google_token_file", metavar="token.json",
                        help="JSON file containing Google API key.")
    parser.add_argument("CADC_cert", metavar="cert.pem",
                        help="CADC cert file (usually ~/.ssl/cadcproxy.pem)")
    args = parser.parse_args()

    
    update_sheet_CADC_tiles(args.CADC_cert,args.Google_token_file)
