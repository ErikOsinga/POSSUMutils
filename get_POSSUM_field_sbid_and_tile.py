#!/usr/bin/env python3
"""
Created on 2025-04-10

This script downloads a survey page (for band 1 or 2) and extracts both "field markers"
and "tile markers" from the JavaScript content. Each marker is defined by an RA, DEC and
various properties (e.g. popupTitle and SBID for field markers). The script then computes
the angular distance (in degrees) between a user-provided target (or coordinates) and each
marker using astropy, and reports separately:
  - The closest field marker (including its field name and SBID)
  - The closest tile marker

Coordinates can be provided directly using the --coords argument (RA DEC in decimal degrees)
or by target name via the --target argument (which queries SIMBAD).

Usage examples:
  python script.py --target "Abell 3627" --band 1
  python script.py --coords 252.5 -41.9 --band 1

Assumes a flat sky approximation so small-scale distortions may be neglected.
"""

import argparse
import sys
import re
import requests
from bs4 import BeautifulSoup
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord
import astropy.units as u

def fetch_html(band):
    """Fetches the HTML content for the given band (1 or 2)."""
    if band == 1:
        url = "https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band1.html"
    elif band == 2:
        url = "https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band2.html"
    else:
        raise ValueError("Band has to be 1 or 2.")
    response = requests.get(url)
    if response.status_code != 200:
        sys.exit(f"Error: Unable to fetch URL {url} (HTTP {response.status_code}).")
    return response.content

def parse_markers(html_content):
    """
    Parses the HTML and extracts field and tile markers.
    
    For field markers, extracts:
      - RA, DEC
      - popupTitle (field name)
      - SBID (if present in popupDesc)
    
    For tile markers, extracts:
      - RA, DEC
      - popupTitle (tile id)
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    # Combine all script text into one string
    script_text = "\n".join(script.get_text() for script in soup.find_all('script'))
    
    field_markers = []
    tile_markers = []
    
    # Regex for field markers (lines that start with field_cat.addSources)
    field_regex = r"field_cat\.addSources\(\[A\.marker\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*\{([^}]+)\}\)\]\);"
    for match in re.findall(field_regex, script_text):
        ra_str, dec_str, props = match
        ra = float(ra_str)
        dec = float(dec_str)
        # Extract popupTitle
        popup_title_match = re.search(r"popupTitle:\s*'([^']+)'", props)
        popup_title = popup_title_match.group(1) if popup_title_match else None
        
        # Extract popupDesc so that we can pull out the SBID value.
        popup_desc_match = re.search(r"popupDesc:\s*'([^']+)'", props)
        popup_desc = popup_desc_match.group(1) if popup_desc_match else ""
        sbid_match = re.search(r"<em>SBID:</em>\s*([^<]+)", popup_desc)
        sbid = sbid_match.group(1).strip() if sbid_match else ""
        
        field_markers.append({
            'ra': ra,
            'dec': dec,
            'popupTitle': popup_title,
            'SBID': sbid
        })
    
    # Regex for tile markers (lines that start with tile_cat.addSources)
    tile_regex = r"tile_cat\.addSources\(\[A\.marker\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*\{([^}]+)\}\)\]\);"
    for match in re.findall(tile_regex, script_text):
        ra_str, dec_str, props = match
        ra = float(ra_str)
        dec = float(dec_str)
        popup_title_match = re.search(r"popupTitle:\s*'([^']+)'", props)
        popup_title = popup_title_match.group(1) if popup_title_match else None
        tile_markers.append({
            'ra': ra,
            'dec': dec,
            'popupTitle': popup_title
        })
    
    return field_markers, tile_markers

def get_coordinates_from_simbad(target_name):
    """
    Queries SIMBAD for the target's coordinates given a target name.
    Returns RA and DEC in decimal degrees.
    """
    simbad = Simbad()
    result_table = simbad.query_object(target_name)
    if result_table is not None and 'ra' in result_table.colnames and 'dec' in result_table.colnames:
        assert result_table['ra'].unit == u.deg
        assert result_table['dec'].unit == u.deg
        coords = SkyCoord(result_table['ra'][0], result_table['dec'][0], unit=(u.deg, u.deg))
        return coords.ra.deg, coords.dec.deg
    else:
        print(f"Error: Target '{target_name}' not found in SIMBAD.")
        sys.exit(1)

def find_closest_marker(target_ra, target_dec, markers):
    """
    Given target coordinates and a list of markers (each with 'ra' and 'dec'),
    returns the marker dictionary that is closest to the target (and adds a
    'separation' key with the angular distance in degrees).
    """
    target_coord = SkyCoord(ra=target_ra * u.deg, dec=target_dec * u.deg)
    closest = None
    min_sep = None
    for marker in markers:
        marker_coord = SkyCoord(ra=marker['ra'] * u.deg, dec=marker['dec'] * u.deg)
        sep = target_coord.separation(marker_coord).deg
        if min_sep is None or sep < min_sep:
            min_sep = sep
            closest = marker
            closest['separation'] = sep
    return closest

def main():
    parser = argparse.ArgumentParser(
        description="Find the closest field marker and tile marker to a given target (by name or coordinates)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--target", type=str,
                       help="Target name for SIMBAD query (e.g. 'Abell 3627').")
    group.add_argument("--coords", nargs=2, type=float, metavar=("RA", "DEC"),
                       help="Directly provide coordinates in decimal degrees.")
    
    parser.add_argument("-b", "--band", type=int, default=1, choices=[1, 2],
                        help="Observing band (1 or 2) to select the correct survey page (default: 1).")
    
    args = parser.parse_args()
    
    # Get target coordinates either via SIMBAD or from direct input
    if args.coords:
        target_ra, target_dec = args.coords
    else:
        target_ra, target_dec = get_coordinates_from_simbad(args.target)
    
    print("================================================")
    print(f"Target Name: {args.target if args.target else 'Provided Coordinates'}")
    print(f"Target Coordinates: RA = {target_ra:.6f} deg, DEC = {target_dec:.6f} deg")
    print("================================================")

    # Fetch and parse the HTML content from the appropriate survey page
    html_content = fetch_html(args.band)
    field_markers, tile_markers = parse_markers(html_content)
    
    # Find and report the closest field marker
    if not field_markers:
        print("No field markers found in the page.")
    else:
        closest_field = find_closest_marker(target_ra, target_dec, field_markers)
        print("Closest Field Marker:")
        print(f"  Field Name (popupTitle): {closest_field.get('popupTitle', 'N/A')}")
        print(f"  SBID: {closest_field.get('SBID', 'N/A')}")
        print(f"  RA: {closest_field.get('ra'):.6f} deg")
        print(f"  DEC: {closest_field.get('dec'):.6f} deg")
        print(f"  Separation: {closest_field.get('separation'):.6f} deg")
    
    # Find and report the closest tile marker
    if not tile_markers:
        print("No tile markers found in the page.")
    else:
        closest_tile = find_closest_marker(target_ra, target_dec, tile_markers)
        print("\nClosest Tile Marker:")
        print(f"  Tile ID (popupTitle): {closest_tile.get('popupTitle', 'N/A')}")
        print(f"  RA: {closest_tile.get('ra'):.6f} deg")
        print(f"  DEC: {closest_tile.get('dec'):.6f} deg")
        print(f"  Separation: {closest_tile.get('separation'):.6f} deg")

if __name__ == '__main__':
    main()
