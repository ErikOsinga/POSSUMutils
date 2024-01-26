"""
Created on 2024-01-25

Query POSSUM status page maintained by Cameron van Eck for some target RA,DEC

Simply scrapes the data from the HTML source page: 
view-source:https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band1.html
and checks whether coordinates fall inside one of the field with a certain status.

Assumes flat sky, so might not work for sources on boundary of observations?
      

Returns:
    True or False

@author: Erik Osinga
"""

import requests
from bs4 import BeautifulSoup
import re
from shapely.geometry import Point, Polygon
from astroquery.simbad import Simbad
from astropy.coordinates import SkyCoord
import astropy.units as u


stages = {'released' : 'validated_field_overlay',
          'observed' : 'observed_field_overlay',
          'processed': 'processed_field_overlay',
          'planned'  : 'field_overlay'
}

def get_overlay_polygons(stage, band=1):
    """
    Find all polygons of a certain class, see above, e.g. 
    
        'validated_field_overlay' - fields that are released
        'observed_field_overlay'  - fields that are observed
        'processed_field_overlay' - fields that are processed
        'field_overlay'           - fields that are planned
    """
    overlay_name = stages[stage]

    # URL of the webpage
    if band == 1:
        url = "https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band1.html"
    elif band == 2:
        url = "https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band2.html"
    else:
        raise ValueError(f"Band has to be 1 or 2, input is {band}")

    # Send an HTTP GET request to the webpage
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content of the webpage
        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract all script tags
        script_tags = soup.find_all('script')

        # Search for the script tags containing the specified overlay
        overlay_polygons = []
        for line in script_tags[2].text.split('\n'):
            if f'{overlay_name}.addFootprints' in line:
                # Define a regular expression pattern to find all floating-point numbers
                pattern = r'-?\d+\.\d+'
                # Use re.findall to extract all matching numbers from the string
                numbers = re.findall(pattern, line)
                # Group the numbers into coordinate pairs
                coordinate_pairs = [(float(numbers[i]), float(numbers[i + 1])) for i in range(0, len(numbers), 2)]

                if coordinate_pairs is not None:
                    overlay_polygons.append(Polygon(coordinate_pairs))

        return overlay_polygons

    return None

def check_coordinates_in_overlay(ra, dec, overlay_name, band=1, overlay_polygons=None):
    # Get the list of polygons corresponding to the specified overlay
    # polygons can be given as input as well to speed up multiple code runs
    if not overlay_polygons:
        overlay_polygons = get_overlay_polygons(overlay_name, band)

    if overlay_polygons:
        # Check if the given RA, DEC falls inside any of the polygons
        point = Point(ra, dec)
        for polygon in overlay_polygons:
            if polygon.contains(point):
                return True

    return False

def get_coordinates_from_simbad(target_name):
    simbad = Simbad()
    result_table = simbad.query_object(target_name)
    if result_table is not None and 'RA' in result_table.colnames and 'DEC' in result_table.colnames:
        # Convert coordinates from sexagesimal to degrees using astropy
        coords = SkyCoord(result_table['RA'][0], result_table['DEC'][0], unit=(u.hourangle, u.deg))
        return coords.ra.deg, coords.dec.deg
    else:
        return None
    
if __name__ == '__main__':
    # Which observing band (1 = 800-1088 MHz, 2 = 1296-1440 MHz)
    band = 1

    # Which status to query
    stage = 'released'
    # stage = 'planned'

    # Example usage with coordinates
    target = None
    ra_input = 252.5
    dec_input = -41.9

    # Example usage with target name, query SIMBAD
    target = 'Abell 85'
    ra_input, dec_input = get_coordinates_from_simbad(target)

    # Compute whether target is in the requested field type
    result = check_coordinates_in_overlay(ra_input, dec_input, stage, band)

    if target:
        print(f"Results of searching for target {target}:")
    if result:
        print(f"The coordinates (RA={ra_input:.3f}, DEC={dec_input:.3f}) fall inside band {band} {stage} fields.")
    else:
        print(f"The coordinates (RA={ra_input:.3f}, DEC={dec_input:.3f}) do not fall inside band {band} {stage} fields.")
        