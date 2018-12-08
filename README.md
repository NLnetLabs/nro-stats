# NRO statistics scripts

Copyright (c) 2018 NLnet Labs (https://nlnetlabs.nl/)

All rights reserved. Distributed under a 3-clause BSD-style license. For more information, see LICENSE

## Introduction

The five Regional Internet Registries (RIRs) maintain NRO statistics, in which they keep track of when blocks of IPv4 and IPv6 space are allocated, assigned (or other actions). These statistics are published every day. Unfortunately, these statistics are not directly useful because of the format in which they are stored, and because they are stored separately by each RIR. In addition to this, the archived data is not always stored in a logical structure.

The goal of the script in this repository is to retrieve the NRO statistics for all RIRs for a specific date and to merge these into a single view, where IP blocks are grouped into the largest possible prefix. The script outputs the joined RIR NRO statistics as two CSV files, one for IPv4 and one for IPv6. NRO statistics are fetch from the archives at the RIPE NCC and the script automatically takes any idiosyncracies of how data is archived into account.

The table below lists the starting dates for the datasets as archived by the RIPE NCC:

|RIR|Starting Date|
|---|-------------|
AfriNIC|March 3rd, 2005|
APNIC|May 1st, 2001|
ARIN|November 20th, 2003|
LACNIC|January 1st, 2004|
RIPE NCC|November 26th, 2003|

This means that the earliest possible date for which the script can build a consolidated view of the global NRO statistics is March 3rd, 2005.

When using this data, it is important to note that the script will replace any missing data by data for the *next* possible available date.

## Dependencies

The script requires Python 3 to run, and has been tested with Python 3.7. The following dependencies need to be installed (available through 'pip'):

 - py-radix >= 0.10

## Running

To run the script, open a shell and execute:

    merge-nrostats.py <date> <output directory>

# Contact

Questions/remarks/suggestions/praise on this tool can be sent to:

Roland van Rijswijk-Deij (<roland@nlnetlabs.nl>)
