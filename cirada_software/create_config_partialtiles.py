#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Creates POSSUM pipeline config file (and working directory) for a single 
tile pipeline run from a given template file.

The output_filename must be different for band 1 and 2, otherwise it'll be
messy down the line to differentiate the configs and the logs.

Created on Wed Apr 17 14:38:47 2024
@author: cvaneck & ErikOsinga
"""

import os
import numpy as np
import ast

def arg_as_list(s):
    v = ast.literal_eval(s)
    if type(v) is not list:
        raise argparse.ArgumentTypeError("Argument \"%s\" is not a list" % (s))
    return v

def modify_config_file(template_filename, field_ID, tilestr, working_dir,
                       output_filename, SB_num):
    with open(template_filename,'r') as f:
        template=np.array(f.readlines())

    #Create working directory (to write config file)
    if not os.path.exists((working_dir)):
        print(f"Creating directory {working_dir} for config file")
        os.makedirs(working_dir)

    template=np.char.replace(template,'[TILESTR]',str(tilestr),count=1)
    template=np.char.replace(template,'[field_ID]',str(field_ID),count=1)
    template=np.char.replace(template,'[SB_num]',str(SB_num),count=1)

    with open(os.path.join(working_dir, output_filename),'w') as f:
        f.writelines(template)

if __name__ == "__main__":
    
    import argparse

    descStr = """
    Generates a POSSUM pipeline config file and working directory from a given template.
    """

    # Parse the command line options
    parser = argparse.ArgumentParser(
        description=descStr,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "template_file",
        metavar="template.ini",
        help="Template pipeline config file (with path if needed).",
    )
    parser.add_argument(
        "output_filename",
        metavar="tile_config.ini",
        help="Output pipeline config file (no path).",
    )
    parser.add_argument(
        "working_dir",
        metavar="working_directory",
        help="Path to working directory. Will create subfolder working_dir/tile1+tile2+tile3+tile4/",
    )
    parser.add_argument(
        "field_ID",
        metavar="field",
        help="Field ID. e.g. 1412-28",
    )
    parser.add_argument(
        "SB_num",
        metavar="SB",
        type=int,
        help="SB number. e.g. 50413",
    )
    parser.add_argument(
        "tilestr",
        metavar="tiles",
        type=str,
        help="Up to Tile numbers as string variable separated by '+'  e.g.: '8843+8971' ",
    )    

    args = parser.parse_args()

    # Create config file from template 
    modify_config_file(args.template_file, args.field_ID, args.tilestr, args.working_dir,
                       args.output_filename, args.SB_num)