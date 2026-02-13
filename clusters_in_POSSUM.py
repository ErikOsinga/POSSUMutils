# clusters_in_POSSUM.py

import numpy as np
import tqdm
from astropy import units as u
from astropy.cosmology import FlatLambdaCDM
from astropy.table import Table
from astroquery.simbad import Simbad

from query_status import (
    check_coordinates_in_overlay,
    get_overlay_polygons,
)

cosmo = FlatLambdaCDM(H0=70, Om0=0.3)


def calculate_R500(M500, redshift):
    R500 = (3 * M500 / (4 * np.pi) / (500 * cosmo.critical_density(redshift))) ** (
        1.0 / 3
    )
    R500 = R500.to(u.kpc)
    return R500.value


# Function to read cluster names from a CSV file
def read_cluster_names_from_file(file, namecol="Name", racol="RAdeg", decol="DEdeg"):
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
    result_table_main = simbad.query_object(target_name)
    result_table_all = simbad.query_objectids(target_name)
    if result_table_main is not None and "MAIN_ID" in result_table_main.colnames:
        mainID = result_table_main["MAIN_ID"][0]
    else:
        mainID = None
        otherIDs = None

    if result_table_all is not None and "ID" in result_table_all.colnames:
        otherIDs = result_table_all["ID"]

    return mainID, otherIDs


# Function to check the status for each cluster in the catalog
def check_clusters_in_possum(catalog_file, stage, band, verbose=False):
    # Read cluster names from the catalogue

    if "PSZ2" in catalog_file:
        cluster_names, ra, dec = read_cluster_names_from_file(catalog_file)
    elif "erass" in catalog_file:
        cluster_names, ra, dec = read_cluster_names_from_file(
            catalog_file, namecol="NAME", racol="RA", decol="DEC"
        )

    # Compute overlay polygons once for this status instead of for every source
    overlay_polygons = get_overlay_polygons(stage, band)

    # Iterate through cluster names and check their status
    inPOSSUM = []
    for i, cluster_name in tqdm.tqdm(
        enumerate(cluster_names), desc="Checking clusters.."
    ):
        ra_input, dec_input = ra[i], dec[i]

        # Check if the coordinates fall inside the specified stage
        result = check_coordinates_in_overlay(
            ra_input, dec_input, stage, band, overlay_polygons
        )

        if result:
            if verbose:
                print(
                    f"The coordinates of '{cluster_name}' fall inside {stage} fields."
                )  # noqa: E701
            inPOSSUM.append(cluster_name)
        else:
            if verbose:
                print(f"Coordinates for '{cluster_name}' not found in {stage} fields.")  # noqa: E701

    return inPOSSUM


# Function to create a subset of the table with only the sources observed in POSSUM
def create_observed_subset(catalog_file, observed_clusters):
    # Read the full table
    full_table = Table.read(catalog_file)
    if "PSZ2" in catalog_file:
        # compatibility
        full_table["M500"] = full_table["MSZ"]
    if "erass" in catalog_file:
        # from 1e13 to 1e14 MSun units
        full_table["M500"] /= 10
        full_table["z"] = full_table["BEST_Z"]
        full_table["Name"] = full_table["NAME"]

    # Create a mask for the observed clusters
    mask = [cluster in observed_clusters for cluster in full_table["Name"]]

    # Create a subset table
    subset_table = full_table[mask]

    return subset_table


def match_names_in_catalog(observed_subset, second_catalog):
    # Extract the 'Name' column from observed_subset
    observed_names = observed_subset["Name"]
    # Extract the 'PSZ2 Name' column from the second catalog
    second_catalog_names = second_catalog["PSZ2 Name"]
    # Remove 'PSZ2 ' part from observed_names for comparison
    observed_names_stripped = [name.replace("PSZ2 ", "") for name in observed_names]
    # Find matching indices in the second catalog
    matching_indices = [
        i
        for i, name in enumerate(second_catalog_names)
        if name in observed_names_stripped
    ]
    # Create a subset table from the second catalog with matching indices
    matched_table = second_catalog[matching_indices]
    return matched_table


# Example usage
if __name__ == "__main__":
    whichcat = "erass"  # 'PSZ2' or 'erass'
    observation_stage = "planned"  # Replace with the desired stage ('released', 'observed', 'processed', 'planned')
    band = 1  # which observing band

    if whichcat == "PSZ2":
        catalog_file = (
            "/home/osingae/Documents/postdoc/downloads/PSZ2_catalogue.dat.fits"
        )
    elif whichcat == "erass":
        catalog_file = "/home/osingae/Documents/postdoc/projects/SURP/Affan/before_project/data/erass1cl_primary_v3.2.fits"
        # catalog_file = '/home/osingae/Documents/postdoc/projects/SURP/data/erass1cl_primary_v3.2.fits'
    else:
        raise ValueError(f"Not yet implemented {whichcat}")

    inPOSSUM = check_clusters_in_possum(catalog_file, observation_stage, band)

    print(
        f"Found {len(inPOSSUM)} clusters inside band {band} fields with status {observation_stage}"
    )

    if "PSZ2" in catalog_file:
        mainnames, allnames = [], []
        for c in tqdm.tqdm(inPOSSUM, desc="Querying SIMBAD for alternative names"):
            mname, anames = get_alternative_names(c)
            mainnames.append(mname)
            allnames.append(anames)
    elif "erass" in catalog_file:
        mainnames = inPOSSUM

    # Get subset of catalogue file
    observed_subset = create_observed_subset(catalog_file, inPOSSUM)
    observed_subset["mainName"] = mainnames
    observed_subset["R500kpc"] = calculate_R500(
        np.array(observed_subset["M500"]) * 1e14 * u.Msun, observed_subset["z"]
    )
    # Convert R500 from kpc to arcmin at each redshift, then to degrees
    arcmin_per_kpc = cosmo.arcsec_per_kpc_proper(observed_subset["z"]).to(
        u.arcmin / u.kpc
    )
    R500_arcmin = (observed_subset["R500kpc"] * arcmin_per_kpc).value  # R500 in arcmin
    R500_deg = R500_arcmin / 60  # Convert arcmin to degrees
    observed_subset["R500deg"] = R500_deg
    # Calculate area, this times 30 is expected number of RMs
    observed_subset["R500_area"] = np.pi * R500_deg**2
    observed_subset["30timesR500_area"] = 30 * observed_subset["R500_area"]
    observed_subset.write(
        f"/home/osingae/Documents/postdoc/projects/SURP/{whichcat}_clusters_band{band}_{observation_stage}.fits",
        overwrite=True,
    )

    # Find a specific cluster doesnt work because there's additional spaces in the name..
    # observed_subset[observed_subset['mainName'] == 'ACO 3104']

    # Compare with my VLA sample
    clusters_vla = Table.read(
        "/home/osingae/Documents/phd/year1/VLA_clusters/final_table_CDS/clusters.fits"
    )

    matched_table = match_names_in_catalog(observed_subset, clusters_vla)
