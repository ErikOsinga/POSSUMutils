#!/usr/bin/env python3
"""
Created on 2025-04-10

This script retrieves survey data from the public Google Sheet containing the POSSUM Processing Status
The relevant tabs are:
  - "Survey Fields - Band 1" and "Survey Fields - Band 2"
  - "Survey Tiles - Band 1" and "Survey Tiles - Band 2"

For field centers the sheet contains:
  - name      : Field identifier
  - ra_deg    : Right Ascension in decimal degrees
  - dec_deg   : Declination in decimal degrees
  - sbid      : SBID of the observation
  - processed, validated, aus_src, single_SB_1D_pipeline: status columns

The status columns are ordered in ascending steps:
  processed < validated < aus_src < single_SB_1D_pipeline,
so if "single_SB_1D_pipeline" contains a value then that field is fully processed.

For tile centers the sheet contains:
  - tile_id   : Tile identifier
  - ra_deg    : Right Ascension in decimal degrees
  - dec_deg   : Declination in decimal degrees
  - aus_src, 3d_pipeline: status columns (with 3d_pipeline indicating the most advanced processing).

The script accepts a target via its name (queried via SIMBAD) or direct coordinates.
It then computes and prints:
  - The closest field center (with its field name, SBID, separation, processing status, and, if applicable,
    links to pipeline data, catalog, FDF and spectra)
  - The closest tile center (with its tile id, separation, and processing status)
                            TODO: add download links for 3D pipeline

Usage examples:
  python script.py --target "Abell 3627" --band 1
  python script.py --coords 252.5 -41.9 --band 1

"""

import argparse
import csv
import io
import re
import sys
from urllib.parse import quote

import astropy.units as u
import requests
from astropy.coordinates import SkyCoord
from astroquery.simbad import Simbad


def remove_prefix(field_name):
    """
    Remove the prefix "EMU_" or "WALLABY_" from the field name.

    e.g.
      s = "EMU_2108-09A"
      remove_prefix(s) -> "2108-09A"
    """
    return re.sub(r"^(EMU_|WALLABY_)", "", field_name)


def fetch_field_centers(band):
    """
    Fetches the field centers from the appropriate Google Sheet tab for the given band.
    The sheet names are "Survey Fields - Band 1" or "Survey Fields - Band 2".
    """
    sheet_name = f"Survey Fields - Band {band}"
    encoded_sheet_name = quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"
    )
    response = requests.get(url)
    if response.status_code != 200:
        sys.exit(
            f"Error: Unable to fetch data from Google Sheets ({response.status_code})."
        )

    csv_data = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_data))
    field_centers = []
    for row in reader:
        try:
            ra = float(row["ra_deg"])
            dec = float(row["dec_deg"])
        except ValueError:
            continue
        marker = {
            "name": row["name"],
            "ra": ra,
            "dec": dec,
            "sbid": row["sbid"],
            "processed": row.get("processed", "").strip(),
            "validated": row.get("validated", "").strip(),
            "aus_src": row.get("aus_src", "").strip(),
            "single_SB_1D_pipeline": row.get("single_SB_1D_pipeline", "").strip(),
        }
        field_centers.append(marker)
    return field_centers


def fetch_tile_centers(band):
    """
    Fetches the tile centers from the appropriate Google Sheet tab for the given band.
    The sheet names are "Survey Tiles - Band 1" or "Survey Tiles - Band 2".
    """
    sheet_name = f"Survey Tiles - Band {band}"
    encoded_sheet_name = quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/"
        f"1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}"
    )
    response = requests.get(url)
    if response.status_code != 200:
        sys.exit(
            f"Error: Unable to fetch data from Google Sheets ({response.status_code})."
        )

    csv_data = response.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_data))
    tile_centers = []
    for row in reader:
        try:
            ra = float(row["ra_deg"])
            dec = float(row["dec_deg"])
        except ValueError:
            continue
        marker = {
            "tile_id": row["tile_id"],
            "ra": ra,
            "dec": dec,
            "aus_src": row.get("aus_src", "").strip(),
            "3d_pipeline": row.get("3d_pipeline", "").strip(),
        }
        tile_centers.append(marker)
    return tile_centers


def get_coordinates_from_simbad(target_name):
    """
    Queries SIMBAD for the target's coordinates given a target name.
    Returns RA and DEC in decimal degrees.
    """
    simbad = Simbad()
    result_table = simbad.query_object(target_name)
    if (
        result_table is not None
        and "ra" in result_table.colnames
        and "dec" in result_table.colnames
    ):
        # SIMBAD returns coordinates in degrees (per the update)
        assert result_table["ra"].unit == u.deg
        assert result_table["dec"].unit == u.deg
        coords = SkyCoord(
            result_table["ra"][0], result_table["dec"][0], unit=(u.deg, u.deg)
        )
        return coords.ra.deg, coords.dec.deg
    else:
        print(f"Error: Target '{target_name}' not found in SIMBAD.")
        sys.exit(1)


def compute_field_status(marker):
    """
    Determines the most relevant status of a field center based on the status columns.
    The hierarchy is (from lowest to highest): processed < validated < aus_src < single_SB_1D_pipeline.
    """
    if marker["single_SB_1D_pipeline"]:
        return "Fully Processed (1D Partial Tile pipeline)"
    elif marker["aus_src"]:
        return "Post-Processed (AUS SRC)"
    elif marker["validated"]:
        return "Validated (POSSUM)"
    elif marker["processed"]:
        return "Processed (ASKAP)"
    else:
        return "Not processed"


def compute_tile_status(marker):
    """
    Determines the most relevant status of a tile center.
    The hierarchy is: aus_src < 3d_pipeline.
    """
    if marker["3d_pipeline"]:
        return "Processed (3D pipeline)"
    elif marker["aus_src"]:
        return "Post-Processed (AUS SRC). 1D/3D pipeline not yet available."
    else:
        return "Not processed (1D/3D pipeline). Tile not yet available"


def find_closest_marker(target_ra, target_dec, markers):
    """
    Given target coordinates and a list of markers (each with 'ra' and 'dec'),
    returns the marker dictionary that is closest to the target (adding a
    'separation' key with the angular distance in degrees).
    """
    target_coord = SkyCoord(ra=target_ra * u.deg, dec=target_dec * u.deg)
    closest = None
    min_sep = None
    for marker in markers:
        marker_coord = SkyCoord(ra=marker["ra"] * u.deg, dec=marker["dec"] * u.deg)
        sep = target_coord.separation(marker_coord).deg
        if min_sep is None or sep < min_sep:
            min_sep = sep
            closest = marker
            closest["separation"] = sep
    return closest


def main():
    parser = argparse.ArgumentParser(
        description="Find the closest survey field center and tile to a given target (by name or coordinates)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-t",
        "--target",
        type=str,
        help="Target name for SIMBAD query (e.g. 'Abell 3627').",
    )
    group.add_argument(
        "--coords",
        nargs=2,
        type=float,
        metavar=("RA", "DEC"),
        help="Directly provide coordinates in decimal degrees.",
    )

    parser.add_argument(
        "-b",
        "--band",
        type=int,
        default=1,
        choices=[1, 2],
        help="Observing band (1 or 2) to select the correct survey sheet (default: 1).",
    )

    parser.add_argument(
        "-maxdist",
        "--maxdist",
        type=float,
        default=4.0,
        help="Maximum distance to the field center. If closest field is further than this, it will not be reported (default: 4.0 degrees).",
    )

    args = parser.parse_args()

    # Get target coordinates via SIMBAD query or direct input
    if args.coords:
        target_ra, target_dec = args.coords
    else:
        target_ra, target_dec = get_coordinates_from_simbad(args.target)

    header = "=" * 60
    print(header)
    print(f"Target: {args.target if args.target else 'Provided Coordinates'}")
    print(f"Coordinates: RA = {target_ra:.6f} deg | DEC = {target_dec:.6f} deg")
    print(header)

    # Fetch field and tile centers from the Google Sheet
    field_centers = fetch_field_centers(args.band)
    tile_centers = fetch_tile_centers(args.band)

    # Determine and report the closest field center
    if not field_centers:
        print("No field centers found. Please check the Google Sheet.")
    else:
        closest_field = find_closest_marker(target_ra, target_dec, field_centers)
        field_status = compute_field_status(closest_field)

        # Check if the closest field is within the maximum distance
        if closest_field["separation"] > args.maxdist:
            print(
                f"\n[Info] Closest field center is {closest_field['separation']:.2f} degrees away, which exceeds the maximum distance of {args.maxdist} degrees."
            )
            print(
                "No POSSUM field center found within the specified maximum distance (--maxdist)."
            )
            sys.exit(0)

        print("Closest Field Center:")
        print("-" * 60)
        print(f" Field Name : {closest_field.get('name', 'N/A')}")
        print(f" SBID       : {closest_field.get('sbid', 'N/A')}")
        print(f" RA         : {closest_field.get('ra'):.6f} deg")
        print(f" DEC        : {closest_field.get('dec'):.6f} deg")
        print(f" Separation : {closest_field.get('separation'):.6f} deg")
        print(f" Status     : {field_status}")

        # If Fully Processed, construct and print pipeline & file links
        if field_status == "Fully Processed (1D Partial Tile pipeline)":
            base_url = "https://www.canfar.net/storage/arc/list/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/"
            band_str = "943MHz" if args.band == 1 else "1367MHz"
            field_id = remove_prefix(closest_field.get("name", ""))
            sbid = closest_field.get("sbid", "").strip()
            pipeline_link = f"{base_url}{band_str}/{field_id}/{sbid}"

            base_url_files = "https://ws-uv.canfar.net/arc/files/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/"
            catalog_link = f"{base_url_files}{band_str}/{field_id}/{sbid}/PSM.{field_id}.{sbid}.catalog.fits"
            fdf_link = f"{base_url_files}{band_str}/{field_id}/{sbid}/PSM.{field_id}.{sbid}.FDF.fits"
            spectra_link = f"{base_url_files}{band_str}/{field_id}/{sbid}/PSM.{field_id}.{sbid}.spectra.fits"

            print("\n Pipeline Data:")
            print(f"  Pipeline Link : {pipeline_link}")
            print("  Additional Files:")
            print(f"   Catalog : {catalog_link}")
            print(f"   FDF     : {fdf_link}")
            print(f"   Spectra : {spectra_link}")

            # Write the extra links to a local file called links_{fieldid}.txt
            filename = f"links_{field_id}.txt"
            with open(filename, "w") as outfile:
                outfile.write("Pipeline Data Links\n")
                outfile.write("=" * 40 + "\n")
                outfile.write(f"Pipeline Link : {pipeline_link}\n")
                outfile.write(f"Catalog Link  : {catalog_link}\n")
                outfile.write(f"FDF Link      : {fdf_link}\n")
                outfile.write(f"Spectra Link  : {spectra_link}\n")
            print(f"\n [Info] Pipeline file links written to: {filename}")

    # Determine and report the closest tile center
    if not tile_centers:
        print("\nNo tile centers found.")
    else:
        closest_tile = find_closest_marker(target_ra, target_dec, tile_centers)
        tile_status = compute_tile_status(closest_tile)
        print("\n" + "=" * 60)
        print("Closest Tile Center:")
        print("-" * 60)
        print(f" Tile ID    : {closest_tile.get('tile_id', 'N/A')}")
        print(f" RA         : {closest_tile.get('ra'):.6f} deg")
        print(f" DEC        : {closest_tile.get('dec'):.6f} deg")
        print(f" Separation : {closest_tile.get('separation'):.6f} deg")
        print(f" Status     : {tile_status}")
        print("=" * 60)


if __name__ == "__main__":
    main()
