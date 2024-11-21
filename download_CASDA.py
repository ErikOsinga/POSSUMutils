import astroquery
from astroquery.casda import Casda
import numpy as np
from astropy.io import ascii
from astropy.coordinates import SkyCoord
import astropy.units as u

# Initialize CASDA object for querying

if int(astroquery.__version__[-1]) < 7:
    ## DONT PUT YOUR PASSWORD ON GITHUB
    casda = Casda("erik.osinga@utoronto.ca","password")
else:
    # After astroquery 0.4.7
    casda = Casda()
    casda.login(username='erik.osinga@utoronto.ca')

# # List of all SBIds observed on 2024-11-15
# sbids = np.loadtxt("/home/osingae/Documents/postdoc/CASDA/SBIDs_observed_2024-11-15.csv",dtype='int')

# Copy of "Survey Observations - Band 1" sheet on 2024-11-15
# obsdata = ascii.read("/home/osingae/Documents/postdoc/CASDA/SurveyObservationsBand1_2024-11-15.csv")
obsdata = ascii.read("/arc/projects/CIRADA/polarimetry/ASKAP/other/SurveyObservationsBand1_2024-11-15.csv")
# Which fields were observed on 2024-11-15
is_observed = np.invert(obsdata['sbid'].mask)

obsdata = obsdata[is_observed]

print(f"Amount of fields observed: {len(obsdata)}")

# for i in range(len(obsdata)):

# i = 0 # doesnt work
i = 1

sc = SkyCoord(obsdata['ra_deg'][i]*u.deg,obsdata['dec_deg'][i]*u.deg)
result = casda.query_region(sc, radius=30*u.arcmin)
# get only POSSUM
result = result[result['obs_collection'] == "POSSUM"]
# get only the file we want, e.g. image.restored.i.EMU_1200-73.SB67157.contcube.conv.fits
result = result[(np.char.startswith(result['filename'], 'image.restored.i.EMU')) & #
                  (np.char.endswith(result['filename'], 'contcube.conv.fits'))
                ]
# get the closest one, which should be the current SBID
idx = np.argmin(result['distance'])
result = result[idx:idx+1] # to make it a 1 length table
assert str(obsdata['sbid'][i]) in result['filename'][0], f"Something went wrong on {i=}"
print(f"Downloading first plane of {result['filename'][0]}")

channel = (1,1) # low and high channel inclusive. Start counting at 1
url_list = casda.cutout(result, channel=channel) # doesnt work for some reason

filelist = casda.download_files(url_list, savedir='./')

