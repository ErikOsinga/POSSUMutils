import numpy as np
from astropy.table import Table
# from astropy.io import fits
import argparse

def write_ds9_regionfile(cat, outfile, racol='RA', deccol='DEC', majcol=None, mincol=None, PAcol=None, majcolunits="deg"):
    """
    Write a DS9 region file from the catalogue located at 'cat'.

    Parameters:
    cat     -- str -- location of .fits catalogue
    outfile -- str -- location of output .reg file
    racol   -- str -- name of RA column in the catalogue (default 'RA')
    deccol  -- str -- name of DEC column in the catalogue (default 'DEC')
    majcol  -- str -- name of major axis column in the catalogue (default None)
    mincol  -- str -- name of minor axis column in the catalogue (default None)
    PAcol   -- str -- name of position angle column in the catalogue (default None)
    """

    # Read the data table
    try:
        tdata = Table.read(cat)#,format='votable')
    except Exception as e:
        print(f"Error reading FITS file '{cat}': {e}")
        return

    # Extract RA and DEC
    try:
        RA = tdata[racol]
        DEC = tdata[deccol]
    except KeyError as e:
        print(f"Column not found in the catalogue: {e}")
        return

    num_sources = len(tdata)
    print(f"Making region with {num_sources} sources")

    # If majcol is provided, use it; else default to 20 arcsec
    if majcol is not None and majcol in tdata.colnames:
        Maj = tdata[majcol]
        if majcolunits == "asec":
            Maj /= 3600
        elif majcolunits == "amin":
            Maj /= 60
    else:
        Maj = np.ones(num_sources) * (20.0 / 3600.0)  # Default to 20 arcsec in degrees

    # If mincol is provided, use it; else default to 20 arcsec
    if mincol is not None and mincol in tdata.colnames:
        Min = tdata[mincol]
        if majcolunits == "asec":
            Min /= 3600
        elif majcolunits == "amin":
            Min /= 60        
    else:
        Min = np.ones(num_sources) * (20.0 / 3600.0)  # Default to 20 arcsec in degrees

    # If PAcol is provided, use it; else default to 0
    if PAcol is not None and PAcol in tdata.colnames:
        PA = tdata[PAcol] + 90.0  # Adjust for DS9's coordinate system
    else:
        PA = np.zeros(num_sources)

    print(f"Writing DS9 region file to '{outfile}'")
    try:
        with open(outfile, 'w') as f:
            # Write header
            f.write('# Region file format: DS9 version 4.1\n')
            f.write('global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" ')
            f.write('select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n')
            f.write('fk5\n')

            # Write regions
            for i in range(num_sources):
                # Handle NaN or zero values
                Maj_value = Maj[i] if not (np.isnan(Maj[i]) or Maj[i] == 0) else (6.0 / 3600.0)
                Min_value = Min[i] if not (np.isnan(Min[i]) or Min[i] == 0) else (6.0 / 3600.0)
                PA_value = PA[i] if not np.isnan(PA[i]) else 0.0

                f.write('ellipse(%.7f, %.7f, %.7f", %.7f", %.2f)\n' %
                        (RA[i], DEC[i], Maj_value * 3600.0, Min_value * 3600.0, PA_value))
    except Exception as e:
        print(f"Error writing to file '{outfile}': {e}")

if __name__ == "__main__":
    """
    e.g.
    
    for POSSUM observatory selavy catalogues    
    python ~/Documents/postdoc/POSSUM/POSSUMutils/table_to_ds9regions.py /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/test_edges/selavy-image.i.EMU_1412-28.SB50413.cont.taylor.0.restored.conv.components.15sig.tiles8843.xml /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/test_edges/selavy-image.i.EMU_1412-28.SB50413.cont.taylor.0.restored.conv.components.15sig.tiles8843.reg --racol col_ra_deg_cont --deccol col_dec_deg_cont


    or for RACS low3 catalogue
    python /home/osingae/Documents/postdoc/POSSUM/POSSUMutils/table_to_ds9regions.py /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/combined_output/racslow3_data/selavy-image.i.RACS_0041-41.SB56983.cont.taylor.0.restored.conv.components.xml /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/combined_output/racslow3_data/selavy-image.i.RACS_0041-41.SB56983.cont.taylor.0.restored.conv.components.reg --racol col_ra_deg_cont --deccol col_dec_deg_cont --majcol col_maj_axis --mincol col_min_axis --PAcol col_pos_ang --majcolunits asec


    or for spiceracs
    python /home/osingae/Documents/postdoc/POSSUM/POSSUMutils/table_to_ds9regions.py /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/combined_output/subcat_matched_POSSUM_SPICERACS_POSSUM_0052-37.fits /home/osingae/Documents/postdoc/CASDA/test_pipeline/test_1dpipeline_2024_11/test_partialtiles/combined_output/subcat_matched_POSSUM_SPICERACS_POSSUM_0052-37.reg --racol ra --deccol dec --majcol maj_axis --mincol min_axis --PAcol pos_ang --majcolunits asec


    """

    # argparse: Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generate a DS9 region file from a FITS catalogue.')
    parser.add_argument('cat', help='Input (FITS) catalogue file.')
    parser.add_argument('outfile', help='Output DS9 region file.')
    parser.add_argument('--racol', default='RA', help='Name of the RA column in the catalogue.')
    parser.add_argument('--deccol', default='DEC', help='Name of the DEC column in the catalogue.')
    parser.add_argument('--majcol', default=None, help='Name of the major axis column in the catalogue.')
    parser.add_argument('--mincol', default=None, help='Name of the minor axis column in the catalogue.')
    parser.add_argument('--PAcol', default=None, help='Name of the position angle column in the catalogue.')
    parser.add_argument('--majcolunits', choices=["deg","amin","asec"], default="deg", help='Units of major and minor axis. Default deg')


    args = parser.parse_args()

    write_ds9_regionfile(
        cat=args.cat,
        outfile=args.outfile,
        racol=args.racol,
        deccol=args.deccol,
        majcol=args.majcol,
        mincol=args.mincol,
        PAcol=args.PAcol,
        majcolunits=args.majcolunits
    )
