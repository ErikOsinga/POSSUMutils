# clusters_in_POSSUM.py

from astropy.io import fits
from astropy.table import Table
from query_status import check_coordinates_in_overlay, get_coordinates_from_simbad, get_overlay_polygons
import tqdm
from astroquery.simbad import Simbad

# Function to read cluster names from a CSV file
def read_cluster_names_from_file(file, namecol='Name', racol = 'RAdeg', decol='DEdeg'):
    tdata = Table.read(file)
    cluster_names = tdata[namecol]
    ra = tdata[racol]
    dec = tdata[decol]
    return cluster_names, ra, dec

# Function to query SIMBAD for alternative names
def get_alternative_names(target_name):
    """
    Returns the main identifier and all other identifiers if the object exists, else None
    """
    simbad = Simbad()
    result_table_main = Simbad.query_object(target_name)
    result_table_all = Simbad.query_objectids(target_name)
    if result_table_main is not None and 'MAIN_ID' in result_table_main.colnames:
        mainID = result_table_main['MAIN_ID'][0]
    else:
        mainID = None

    if result_table_all is not None and 'ID' in result_table_all.colnames:
        otherIDs = result_table_all['ID']

    return mainID, otherIDs

# Function to check the status for each cluster in the catalog
def check_clusters_in_possum(catalog_file, stage, band, verbose=False):
    # Read cluster names from the catalogue
    cluster_names, ra, dec = read_cluster_names_from_file(catalog_file)

    # Compute overlay polygons once for this status instead of for every source
    overlay_polygons = get_overlay_polygons(stage, band)

    # Iterate through cluster names and check their status
    inPOSSUM = []
    for i, cluster_name in tqdm.tqdm(enumerate(cluster_names),desc='Checking clusters..'):
        ra_input, dec_input = ra[i], dec[i]

        # Check if the coordinates fall inside the specified stage
        result = check_coordinates_in_overlay(ra_input, dec_input, stage, band, overlay_polygons)

        if result:
            if verbose: print(f"The coordinates of '{cluster_name}' fall inside {stage} fields.")
            inPOSSUM.append(cluster_name)
        else:
            if verbose: print(f"Coordinates for '{cluster_name}' not found in SIMBAD.")

    return inPOSSUM


# Example usage
if __name__ == '__main__':
    catalog_file = '/home/osingae/Documents/postdoc/downloads/PSZ2_catalogue.dat.fits'
    observation_stage = 'released'  # Replace with the desired stage ('released', 'observed', 'processed', 'planned')
    band = 1 # which observing band

    inPOSSUM = check_clusters_in_possum(catalog_file, observation_stage, band)

    print(f"Found {len(inPOSSUM)} clusters inside band {band} fields with status {observation_stage}")

    mainnames, allnames = [], []
    for c in inPOSSUM:
        mname, anames = get_alternative_names(c)
        mainnames.append(mname)
        allnames.append(anames)
    
