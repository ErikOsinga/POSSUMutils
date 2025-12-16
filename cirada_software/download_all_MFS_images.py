#!/usr/bin/env python
import numpy as np
from astroquery.utils.tap.core import Tap
from astroquery.casda import Casda
import argparse
import os
import getpass
import re
import gspread
import astropy.table as at
import tqdm


def get_all_sbids(band, Google_API_token):
    """
    Connects to the POSSUM Status Monitor Google Sheet and returns a sub-table
    containing only the rows where the 'sbid' field is not empty.

    Uses the following Google API token on canfar:
        "/arc/home/ErikOsinga/.ssh/psm_gspread_token.json"

    And the sheet URL:
        https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0

    The worksheet is selected based on the band:
        '1' if band == '943MHz', otherwise '2'.
    """

    # Authenticate and grab the spreadsheet
    gc = gspread.service_account(filename=Google_API_token)
    ps = gc.open_by_url(
        "https://docs.google.com/spreadsheets/d/1sWCtxSSzTwjYjhxr1_KVLWG2AnrHwSJf_RWQow7wbH0"
    )

    # Select the worksheet for the given band number
    band_number = "1" if band == "943MHz" else "2"
    tile_sheet = ps.worksheet(f"Survey Fields - Band {band_number}")
    tile_data = tile_sheet.get_all_values()
    column_names = tile_data[0]
    tile_table = at.Table(np.array(tile_data)[1:], names=column_names)

    # Find all rows that have "sbid" not empty and "aus_src" not empty and 'single_SB_1D_pipeline' empty
    mask = tile_table["sbid"] != ""
    ready_table = tile_table[mask]
    return ready_table


def download(url_list, sb):
    for url in url_list:
        if url.find("checksum") != -1:
            continue
        print("Processing %s" % (url))
        # -P --directory-prefix=sb   -- save files to ./sb/
        cmd = "wget -P " + sb + ' --content-disposition "%s"' % (url)
        print(cmd)
        os.system(cmd)
        # os.system("wget --content-disposition \"%s\" -O CMPL.tar" %(url))
        # os.system("tar -xvf CMPL.tar")


def getResultsForFilenamePart(results, fileNamePart):
    regex = re.compile(fileNamePart)
    files = results["filename"].tolist()
    match = [string for string in files if re.match(regex, string)]
    i = files.index(match[0])
    return results[i]


def getImageURLs(casda_tap, sb):
    images = casda_tap.launch_job_async(
        "SELECT * FROM ivoa.obscore WHERE obs_id='ASKAP-%s'" % (sb)
    )
    r_images = images.get_results()

    results = []
    # results.append(getResultsForFilenamePart(r_images, 'selavy-image.i.EMU_*.*.cont.taylor.0.restored.conv.components.xml'))
    # results.append(getResultsForFilenamePart(r_images, 'selavy-image.i.EMU_*.*.cont.taylor.0.restored.conv.polarisation.xml'))
    # results.append(getResultsForFilenamePart(r_images, 'image.restored.i.*.contcube.conv.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'image.restored.q.*.contcube.conv.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'image.restored.u.*.contcube.conv.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'image.restored.v.*.contcube.conv.fits'))
    results.append(
        getResultsForFilenamePart(
            r_images, "image.i.*.cont.taylor.0.restored.conv.fits"
        )
    )
    # results.append(getResultsForFilenamePart(r_images, 'image.i.*.cont.taylor.1.restored.conv.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.i.*.contcube.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.q.*.contcube.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.u.*.contcube.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.v.*.contcube.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.i.*.cont.taylor.0.fits'))
    # results.append(getResultsForFilenamePart(r_images, 'residual.i.*.cont.taylor.1.fits'))
    image_urls = casda.stage_data(results, verbose=True)
    return image_urls


def getEvalURLs(casda_tap, sb):
    evalfiles = casda_tap.launch_job_async(
        "SELECT * FROM casda.observation_evaluation_file oef inner join casda.observation o on oef.observation_id = o.id where o.sbid = '%s'"
        % (sb)
    )
    r_eval = evalfiles.get_results()
    evals = []
    evals.append(getResultsForFilenamePart(r_eval, "BeamwiseSourcefinding"))
    evals.append(getResultsForFilenamePart(r_eval, "calibration-metadata-processing"))
    tar_urls = casda.stage_data(evals, verbose=True)
    return tar_urls


if __name__ == "__main__":
    # POSSUM Status Monitor
    # on CANFAR
    Google_API_token = "/arc/home/ErikOsinga/.ssh/psm_gspread_token.json"

    parser = argparse.ArgumentParser(description="Download all EMU MFS images")
    parser.add_argument(
        "--psm_api_token",
        type=str,
        default=Google_API_token,
        help="Path to POSSUM status sheet Google API token JSON file",
    )
    args = parser.parse_args()

    Google_API_token = args.psm_api_token

    band = "943MHz"  # hardcode for now

    username = "erik.osinga@utoronto.ca"

    casda_tap = Tap(url="https://casda.csiro.au/casda_vo_tools/tap")
    # passw = getpass.getpass(str("Enter password for user "+username+": "))
    casda = Casda()
    casda.login(username=username)

    # Get which fields are ready for processing from aussrc
    ready_table = get_all_sbids(band, Google_API_token)
    sbids = ready_table["sbid"]

    # Make sure we download in here, create subfolder for every MFS image (numbered by SBID)
    os.chdir("/arc/projects/CIRADA/polarimetry/ASKAP/single_SB_pipeline/mfs_images/")

    for sb in tqdm.tqdm(sbids, desc="Downloading MFS images"):
        os.makedirs(sb, exist_ok=True)
        # os.system("mkdir "+sb, exist_ok=True)

        # Check if the directory already contains a .fits image
        if any(fname.endswith(".fits") for fname in os.listdir(sb)):
            print(f"Directory {sb} already contains .fits images. Skipping download.")
            continue

        # os.chdir(sb)
        imageURLs = getImageURLs(casda_tap, sb)
        # evalURLs = getEvalURLs(casda_tap, sb)
        print(f"Downloading image files for SBID = {sb}")
        download(imageURLs, sb)
        # print(f'Downloading evaluation files for SBID = {sb}')
        # download(evalURLs, sb)

        # os.chdir('/arc/projects/CIRADA/polarimetry/ASKAP/single_SB_pipeline/mfs_images/')

    print("All MFS images downloaded.")
