# POSSUMutils
A set of control tools related to running the POSSUM 1D and 3D pipelines on the CANFAR science platform. 

With everything set up (credentials, pip install -r requirements.txt) on the virtual machine, start running the 1D pipeline as follows:

```
screen -r 1Dpipeline # or screen -S 1Dpipeline the first time
cd POSSUMutils/
bash go_1Dpipeline.sh
```

or the 3D pipeline:

```
screen -r 3Dpipeline # or screen -S 3Dpipeline the first time
cd POSSUMutils/
bash go_3Dpipeline
```


## cirada_software/

A set of scripts meant to live on CANFAR where they can be called to start headless jobs.


## possum_pipeline_control/

A set of scripts meant to live on the virtual machine 'p1' to operate the control logic for launching POSSUM pipelines on CANFAR.


## root directory (./)
For quick looks at the POSSUM processing status or data availability.

### Grab closest POSSUM field and tile
```get_POSSUM_field_sbid_and_tile.py``` 

Query POSSUM status sheet for some target (name) or RA,DEC. Returns the closest field and tile, whether they've been processed, and direct download links to the data (requires CANFAR login). 

Example usage:

`python get_POSSUM_field_sbid_and_tile.py -t "Abell 3627"`

or

`python get_POSSUM_field_sbid_and_tile.py --coords 246.85 -60.32`


### Query POSSUM status
```query_status.py``` 

Query POSSUM status page maintained by Cameron van Eck for some target RA,DEC

Simply scrapes the data from the HTML source page: 
view-source:https://www.mso.anu.edu.au/~cvaneck/possum/aladin_survey_band1.html
and checks whether coordinates fall inside one of the field with a certain status.

Assumes flat sky, so might not work for sources on boundary of observations? Probably superseded by `get_POSSUM_field_sbid_and_tile.py`


## handy_scripts/

Some handy scripts not necessarily related to POSSUM.



## TODO:

- Should move to dev/test/prod env for the pipelines
- Should implement CANDIAPL best practices, and make this a proper python package.

