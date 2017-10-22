#! /usr/bin/env python
"""
read_fast5.py: A library for reading FAST5 files.

Author: Katie Doroschak

Functions:

read_fast5_channel
        Reads a FAST5 file of nanopore current information.
"""

import h5py


def read_fast5(fast5_file, raw_loc=None, metadata_loc=None):
    f5 = h5py.File(fast5_file)
    current = get_scaled_raw_current(f5, raw_loc=raw_loc,
                                     metadata_loc=metadata_loc)
    sample_rate = get_sample_rate(f5)  # Datapoints per second (assumption)
    return sample_rate, current


def get_sample_rate(f5):
    try:
        sample_rate = f5.get("Meta").attrs['sample_rate']
        return sample_rate
    except AttributeError:
        pass
    try:
        file_data = f5.get("UniqueGlobalKey")
        sample_rate = int(file_data.get("context_tags")
                          .attrs["sample_frequency"])
        return sample_rate
    except AttributeError:
        pass
    raise ValueError("Cannot find sample rate.")


def get_scaled_raw_current(f5, raw_loc=None, metadata_loc=None):
    default_raw_locs = ["/Raw/Channel_<>/Signal",
                        "/Raw/Reads/Read_<>/Signal"]
    default_metadata_locs = ["/Raw/Channel_<>/Meta",
                             "/UniqueGlobalKey/channel_id"]
    raw = None
    if raw_loc is not None:
        raw = get_raw(f5, raw_loc)
    else:
        for default_loc in default_raw_locs:
            try_raw = get_raw(f5, default_loc)
            if try_raw is not None:
                raw = try_raw
                break
    if raw is None:
        raise ValueError("Could not find raw signal at the given f5 directory."
                         " (If the directory was not given, then it could not "
                         " be found in the default directories.")

    offset, rng, digitisation = None, None, None
    if metadata_loc is not None:
        offset, rng, digitisation = get_scale_factors(f5, metadata_loc)
    else:
        for default_loc in default_metadata_locs:
            meta = get_scale_factors(f5, default_loc)
            if meta is not None:
                offset, rng, digitisation = meta
                break
    if offset is None or rng is None or digitisation is None:
        raise ValueError("Could not find the scaling factors in the given f5 "
                         "directory. (If the directory was not given, then it "
                         "could not be found in the default directories.")

    current = scale_raw_current(raw, offset, rng, digitisation)
    return current


def get_raw(f5, loc):
    '''
    Retrieve the raw current from the chain of groups in the open f5 h5py file.
    '''
    loc = loc.split("/")
    raw = f5
    for group in loc:
        if len(group) == 0:
            continue
        if "<>" in group:
            g = raw.values()
            if len(g) > 1:
                # TODO warn about choosing first group or send error?
                print "Warning: More than 1 possible group, choosing the first"
            elif len(g) == 0:
                print "Error: No subgroups of group:", group
                return None
            raw = g[0]
            continue
        try:
            raw = raw.get(group)
        except AttributeError:
            e = "Could not find group:", group
            print e
            # raise AttributeError(e)
            return None
    try:
        raw = raw[:]
    except TypeError:
        return None
    return raw


def get_scale_factors(f5, loc):
    '''
    Retrieve the offset, range, and digitisation (sic., UK sp.) from the chain
    of groups in the open f5 h5py file.
    '''
    loc = loc.split("/")
    data = f5
    for group in loc:
        if len(group) == 0:
            continue
        if "<>" in group:
            g = data.values()
            if len(g) > 1:
                # TODO warn about choosing first group or send error?
                print "Warning: More than 1 possible group, choosing the first"
            elif len(g) == 0:
                print "Error: No subgroups of group:", group
                return None
            data = g[0]
            continue
        try:
            data = data.get(group)
        except AttributeError:
            e = "Could not find group:", group
            # raise AttributeError(e)
            print e
            return None
    try:
        offset = data.attrs["offset"]
        rng = data.attrs["range"]
        digitisation = data.attrs["digitisation"]
    except (AttributeError, KeyError):
        return None
    return offset, rng, digitisation


# def get_scaled_raw_for_channel(f5, channel=None):
#     '''Note: using UK sp. of digitization for consistency w/ file format'''
#     fmt = '''FAST5 internal format:
#         FILE_CONTENTS {
#          group      /
#          group      /Device
#          dataset    /Device/AsicCommands
#          dataset    /Device/MetaData
#          group      /Meta
#          group      /Meta/User
#          dataset    /Meta/User/analysis_conf
#          group      /Raw
#          group      /Raw/Channel_X
#          group      /Raw/Channel_X/Meta
#          dataset    /Raw/Channel_X/Signal
#          }'''
#     try:
#         if channel is None:
#             # Get the list of channels
#             # If there's only one, pick it & continue
#             # If there are more than 1, error w/ possible channels
#             pass
#         channel_data = f5.get("Raw").get(channel)
#         raw = channel_data.get("Signal").value
#         offset = channel_data.get("Meta").attrs["offset"]
#         rng = channel_data.get("Meta").attrs["range"]
#         digitisation = channel_data.get("Meta").attrs["digitisation"]
#         return scale_raw_current(raw, offset, rng, digitisation)
#     except AttributeError:
#         new_e = "Possible wrong file format, expecting:\n" + fmt + "\n"
#         new_e += "where \"Meta\" contains the offset, range, and digitisation."
#         raise AttributeError(new_e)
#         return None  # Wrong file format


# def get_scaled_raw_for_read(f5, read_name):
#     '''Note: using UK sp. of digitization for consistency w/ file format'''
#     fmt = '''FAST5 internal format:
#         FILE_CONTENTS {
#          group      /
#          group      /Raw
#          group      /Raw/Reads
#          group      /Raw/Reads/Read_X
#          dataset    /Raw/Reads/Read_X/Signal
#          group      /UniqueGlobalKey
#          group      /UniqueGlobalKey/channel_id
#          group      /UniqueGlobalKey/context_tags
#          group      /UniqueGlobalKey/tracking_id
#          }'''
#     try:
#         raw = f5.get("Raw").get("Reads").get(read_name).get("Signal")
#         channel_data = f5.get("UniqueGlobalKey").get("channel_id")
#         offset = channel_data.attr["offset"]
#         rng = channel_data.attr["range"]
#         digitisation = channel_data.attr["digitisation"]
#         return scale_raw_current(raw, offset, rng, digitisation)
#     except AttributeError:
#         new_e = "Possible wrong file format, expecting:\n" + fmt + "\n"
#         new_e += "where \"UniqueGlobalKey/channel_id\" contains the offset, "
#         new_e += "range, and digitisation. Also double check your read name."
#         raise AttributeError(new_e)
#         return None  # Wrong file format


def scale_raw_current(raw, offset, rng, digitisation):
    '''Note: using UK sp. of digitization for consistency w/ file format'''
    return (raw + offset) * (rng / digitisation)
