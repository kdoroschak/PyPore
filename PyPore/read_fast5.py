#! /usr/bin/env python
"""
read_fast5.py: A library for reading ABF files.

Author: Katie Doroschak

Functions:

read_current
        Reads an abf file of nanopore current information.
"""


import h5py
# import numpy as np
# import re
import logging


def read_fast5_channel(fast5_file, channel):
    with open(fast5_file, "r") as f:
        f5 = h5py.File(fast5_file)

    current = get_scaled_raw_for_channel(f5, channel)

    return time_step_msec, current


def get_sample_rate(f5):
    try:
        sample_rate = f5.get("Meta").attrs['sample_rate']
        return sample_rate
    except AttributeError:
        pass
    try:
        file_data = f5.get("UniqueGlobalKey")
        sample_rate = file_data.get("context_tags").attrs["sample_frequency"]
        return sample_rate
    except AttributeError:
        pass
    raise ValueError("Cannot find sample rate.")


def get_scaled_raw_for_channel(f5, channel):
    '''Note: using UK sp. of digitization for consistency w/ file format'''
    fmt = '''FAST5 internal format:
        FILE_CONTENTS {
         group      /
         group      /Device
         dataset    /Device/AsicCommands
         dataset    /Device/MetaData
         group      /Meta
         group      /Meta/User
         dataset    /Meta/User/analysis_conf
         group      /Raw
         group      /Raw/Channel_X
         group      /Raw/Channel_X/Meta
         dataset    /Raw/Channel_X/Signal
         }'''
    try:
        channel_data = f5.get("Raw").get(channel)
        raw = channel_data.get("Signal").value
        offset = channel_data.get("Meta").attrs["offset"]
        rng = channel_data.get("Meta").attrs["range"]
        digitisation = channel_data.get("Meta").attrs["digitisation"]
        return scale_raw_current(raw, offset, rng, digitisation)
    except AttributeError:
        new_e = "Possible wrong file format, expecting:\n" + fmt + "\n"
        new_e += "where \"Meta\" contains the offset, range, and digitisation."
        raise AttributeError(new_e)
        return None  # Wrong file format


def get_scaled_raw_for_read(f5, read_name):
    '''Note: using UK sp. of digitization for consistency w/ file format'''
    fmt = '''FAST5 internal format:
        FILE_CONTENTS {
         group      /
         group      /Raw
         group      /Raw/Reads
         group      /Raw/Reads/Read_X
         dataset    /Raw/Reads/Read_X/Signal
         group      /UniqueGlobalKey
         group      /UniqueGlobalKey/channel_id
         group      /UniqueGlobalKey/context_tags
         group      /UniqueGlobalKey/tracking_id
         }'''
    try:
        raw = f5.get("Raw").get("Reads").get(read_name).get("Signal")
        channel_data = f5.get("UniqueGlobalKey").get("channel_id")
        offset = channel_data.attr["offset"]
        rng = channel_data.attr["range"]
        digitisation = channel_data.attr["digitisation"]
        return scale_raw_current(raw, offset, rng, digitisation)
    except AttributeError:
        new_e = "Possible wrong file format, expecting:\n" + fmt + "\n"
        new_e += "where \"UniqueGlobalKey/channel_id\" contains the offset, "
        new_e += "range, and digitisation. Also double check your read name."
        raise AttributeError(new_e)
        return None  # Wrong file format



def scale_raw_current(raw, offset, rng, digitisation):
    '''Note: using UK sp. of digitization for consistency w/ file format'''
    return (raw + offset) * (rng / digitisation)
