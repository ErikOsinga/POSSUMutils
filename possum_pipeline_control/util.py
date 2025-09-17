"""
Utility methods shared across the scripts
"""

def get_band_number(band):
    """
    Convert band string to band number.
    """
    return '1' if band == '943MHz' else '2'

def get_full_field_name(field_ID, band):
    """
    Build field name prefix
    """
    fieldname = "EMU_" if band == '943MHz' else 'WALLABY_'  # TODO: verify WALLABY_ fieldname
    return f"{fieldname}{field_ID}"
