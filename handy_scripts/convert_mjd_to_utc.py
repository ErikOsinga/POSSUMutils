from astropy.time import Time
import argparse


def convert_mjd_to_utc(mjd):
    """
    Convert Modified Julian Date to UTC
    """
    # Convert to Julian Date
    jd = mjd + 2400000.5

    # Convert to astropy Time object
    t = Time(jd, format='jd')

    # Convert to datetime
    utc = t.to_datetime()

    return utc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert MJD to UTC')
    parser.add_argument('mjd', type=float, help='Modified Julian Date')
    args = parser.parse_args()

    mjd = args.mjd

    utc = convert_mjd_to_utc(mjd)
    
    print(f'MJD: {mjd}')
    print(f'UTC: {utc}')