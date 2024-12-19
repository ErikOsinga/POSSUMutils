# POSSUMutils
A set of handy scripts related to POSSUM


## Main directory

query_status.py -- Query POSSUM status page maintained by Cameron van Eck for some target RA,DEC

Simply scrapes the data from the HTML source page: 
view-source:https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band1.html
and checks whether coordinates fall inside one of the field with a certain status.

Assumes flat sky, so might not work for sources on boundary of observations?



## cirada_software/

A set of scripts meant to live on CANFAR where they can be called to start headless jobs 


## possum_pipeline_control

A set of scripts meant to live on 'p1' meant to control logic for launching POSSUM pipelines on CANFAR


## handy_scripts/

Some handy scripts not necessarily related to POSSUM

