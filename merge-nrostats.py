#!/usr/bin/env python3
#
# Copyright (c) 2018 NLnet Labs
# Licensed under a 3-clause BSD license, see LICENSE in the
# distribution
#
# This script retrieves the NRO statistics from the RIPE FTP server
# and merges the statistics for the individual RIRs into a single
# output file. It will only process ipv4 and ipv6 prefixes and
# will output data in the same format as the NRO stats
#
# This script may require you to install the following dependencies:
# - py-radix
#
# All of these dependencies are available through 'pip'

import os
import os.path
import sys
import datetime
import dateutil.parser
import requests
import radix
import gzip
from io import StringIO
import bz2
import math
import ipaddress
import urllib.request

##
# Configuration
##

ripe_base_url	= 'https://ftp.ripe.net/pub/stats'
afrinic_sub	= 'afrinic'
arin_sub	= 'arin'
apnic_sub	= 'apnic'
lacnic_sub	= 'lacnic'
ripencc_sub	= 'ripencc'

tmpdir		= '/tmp'

# Retrieve NRO stats for the AfriNIC region for the
# specified date
def fetch_afrinic_nrostats(day):
	# Where do the archives at the RIPE NCC
	start_date = datetime.date(2005,3,3)

	if day < start_date:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/afrinic-empty'.format(tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	# The archive at the RIPE NCC misses data for 
	# certain dates which we replace with newer
	# data
	missing_dateranges = []
	missing_dateranges.append((datetime.date(2011,1,1), datetime.date(2011,5,15), datetime.date(2011,5,16)))
	missing_dateranges.append((datetime.date(2014,12,31), datetime.date(2015,1,6), datetime.date(2015,1,7)))
	missing_dateranges.append((datetime.date(2015,12,31), datetime.date(2016,1,4), datetime.date(2016,1,5)))
	missing_dateranges.append((datetime.date(2016,12,31), datetime.date(2016,12,31), datetime.date(2017,1,1)))
	missing_dateranges.append((datetime.date(2017,12,31), datetime.date(2018,1,3), datetime.date(2018,1,4)))

	for daterange in missing_dateranges:
		if day >= daterange[0] and day <= daterange[1]:
			print('{} is missing for AfriNIC, replacing it by {}'.format(day, daterange[2]))
			day = daterange[2]

	# Some days ended up in different years, note
	# these exceptions
	exception_dates = []
	exception_dates.append((datetime.date(2012,12,31), 2013))

	year = day.year

	for exception in exception_dates:
		if day == exception[0]:
			print('{} is in another year directory for AfriNIC, adjusting year directory to {}'.format(day, exception[1]))
			year = exception[1]

	afrinic_url = '{}/{}/{}/delegated-afrinic-{:04d}{:02d}{:02d}'.format(ripe_base_url, afrinic_sub, year, day.year, day.month, day.day)

	sys.stdout.write('Fetching AfriNIC data from {} ... '.format(afrinic_url))
	sys.stdout.flush()

	tmpfile = '{}/afrinic-{:04d}{:02d}{:02d}'.format(tmpdir, day.year, day.month, day.day)

	try:
		urllib.request.urlretrieve(afrinic_url, tmpfile)
	except:
		raise Exception('Failed to download {} to {}'.format(afrinic_url, tmpfile))

	print('OK ({})'.format(tmpfile))

	return tmpfile

# Retrieve NRO stats for the ARIN region for
# the specified date
def fetch_arin_nrostats(day):
	# Where do the archives at the RIPE NCC
	start_date = datetime.date(2003,11,20)

	if day < start_date:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/arin-empty'.format(tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	# The ARIN stats are a bit of a mess unfortunately
	# so determining the right URL requires some black
	# magic. We assume that ARIN always has a full year
	# of data in the current dir + any data from the
	# running year, and that everything else is under
	# the "archive" directory. Furthermore, the file
	# format and file naming scheme change as of
	# March 5, 2013. To top it of, until September 30,
	# 2007, the files are gzipped, after that, they're
	# just plain ASCII text.
    #
    # 2020-04-30:
    # Turns out that from 2017 onward the ARIN files
    # do reside in a single directory and not in the
    # "archive" directory... go figure...
	subdir = None
	filename_prefix = None
	filename_postfix = None
	
	today = datetime.date.today()

	if day.year < 2017:
		subdir = 'archive/{}'.format(day.year)

	# The archive at the RIPE NCC misses data for 
	# certain dates which we replace with newer
	# data
	missing_dateranges = []
	missing_dateranges.append((datetime.date(2019,8,25), datetime.date(2019,8,25), datetime.date(2019,8,26)))

	for daterange in missing_dateranges:
		if day >= daterange[0] and day <= daterange[1]:
			print('{} is missing for ARIN, replacing it by {}'.format(day, daterange[2]))
			day = daterange[2]

	if day >= datetime.date(2013,3,5):
		filename_prefix = 'delegated-arin-extended'
	else:
		filename_prefix = 'delegated-arin'

	if day <= datetime.date(2007,9,30):
		filename_postfix = '.gz'

	arin_url = '{}/{}'.format(ripe_base_url, arin_sub)

	if subdir is not None:
		arin_url += '/{}'.format(subdir)

	arin_url += '/{}-{:04d}{:02d}{:02d}'.format(filename_prefix, day.year, day.month, day.day)

	if filename_postfix is not None:
		arin_url += filename_postfix

	sys.stdout.write('Fetching ARIN data from {} ... '.format(arin_url))
	sys.stdout.flush()

	tmpfile = '{}/arin-{:04d}{:02d}{:02d}'.format(tmpdir, day.year, day.month, day.day)

	if filename_postfix is not None:
		tmpfile += filename_postfix

	try:
		urllib.request.urlretrieve(arin_url, tmpfile)
	except:
		raise Exception('Failed to download {} to {}'.format(arin_url, tmpfile))

	print('OK ({})'.format(tmpfile))

	return tmpfile

# Retrieve NRO stats for the APNIC region for
# the specified date
def fetch_apnic_nrostats(day):
	# Where do the archives at the RIPE NCC
	start_date = datetime.date(2001,5,1)

	if day < start_date:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/apnic-empty'.format(tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	# The archive at the RIPE NCC misses data for 
	# certain dates which we replace with newer
	# data
	missing_dateranges = []
	missing_dateranges.append((datetime.date(2001,5,2), datetime.date(2001,5,31), datetime.date(2001,6,1)))
	missing_dateranges.append((datetime.date(2001,6,2), datetime.date(2001,8,31), datetime.date(2001,9,1)))
	missing_dateranges.append((datetime.date(2001,9,2), datetime.date(2001,9,30), datetime.date(2001,10,1)))
	missing_dateranges.append((datetime.date(2001,10,2), datetime.date(2001,10,31), datetime.date(2001,11,1)))
	missing_dateranges.append((datetime.date(2001,11,2), datetime.date(2001,11,30), datetime.date(2001,12,1)))
	missing_dateranges.append((datetime.date(2001,12,2), datetime.date(2001,12,31), datetime.date(2002,1,1)))
	missing_dateranges.append((datetime.date(2002,1,2), datetime.date(2002,1,31), datetime.date(2002,2,1)))
	missing_dateranges.append((datetime.date(2002,2,2), datetime.date(2002,2,28), datetime.date(2002,3,1)))
	missing_dateranges.append((datetime.date(2002,3,2), datetime.date(2002,3,31), datetime.date(2002,4,1)))
	missing_dateranges.append((datetime.date(2002,4,2), datetime.date(2002,4,30), datetime.date(2002,5,1)))
	missing_dateranges.append((datetime.date(2002,5,2), datetime.date(2002,5,31), datetime.date(2002,6,1)))
	missing_dateranges.append((datetime.date(2002,6,2), datetime.date(2002,6,30), datetime.date(2002,7,1)))
	missing_dateranges.append((datetime.date(2002,7,2), datetime.date(2002,7,31), datetime.date(2002,8,1)))
	missing_dateranges.append((datetime.date(2002,8,2), datetime.date(2002,8,31), datetime.date(2002,9,1)))
	missing_dateranges.append((datetime.date(2002,9,2), datetime.date(2002,9,30), datetime.date(2002,10,1)))
	missing_dateranges.append((datetime.date(2002,10,2), datetime.date(2002,10,31), datetime.date(2002,11,1)))
	missing_dateranges.append((datetime.date(2002,11,2), datetime.date(2002,11,30), datetime.date(2002,12,1)))
	missing_dateranges.append((datetime.date(2002,12,2), datetime.date(2002,12,31), datetime.date(2003,1,1)))
	missing_dateranges.append((datetime.date(2003,1,2), datetime.date(2003,1,31), datetime.date(2003,2,1)))
	missing_dateranges.append((datetime.date(2003,2,2), datetime.date(2003,2,28), datetime.date(2003,3,1)))
	missing_dateranges.append((datetime.date(2003,3,2), datetime.date(2003,3,31), datetime.date(2003,4,1)))
	missing_dateranges.append((datetime.date(2003,4,2), datetime.date(2003,4,30), datetime.date(2003,5,1)))
	missing_dateranges.append((datetime.date(2003,5,2), datetime.date(2003,5,7), datetime.date(2003,5,8)))

	for daterange in missing_dateranges:
		if day >= daterange[0] and day <= daterange[1]:
			print('{} is missing for APNIC, replacing it by {}'.format(day, daterange[2]))
			day = daterange[2]

	# Some days ended up in different years, note
	# these exceptions
	exception_dates = []
	exception_dates.append((datetime.date(2010,12,31), 2011))
	exception_dates.append((datetime.date(2011,12,31), 2012))
	exception_dates.append((datetime.date(2012,12,31), 2013))
	exception_dates.append((datetime.date(2013,12,31), 2014))
	exception_dates.append((datetime.date(2014,12,31), 2015))
	exception_dates.append((datetime.date(2015,12,31), 2016))
	exception_dates.append((datetime.date(2016,12,31), 2017))
	exception_dates.append((datetime.date(2017,12,31), 2018))
	exception_dates.append((datetime.date(2018,12,31), 2019))

	year = day.year

	for exception in exception_dates:
		if day == exception[0]:
			print('{} is in another year directory for APNIC, adjusting year directory to {}'.format(day, exception[1]))
			year = exception[1]

	# The filename for APNIC is different before
	# October 9, 2003
	filename = None

	if day <= datetime.date(2003,10,8):
		filename = 'apnic-{:04d}-{:02d}-{:02d}.gz'.format(day.year, day.month, day.day)
	else:
		filename = 'delegated-apnic-{:04d}{:02d}{:02d}.gz'.format(day.year, day.month, day.day)

	apnic_url = '{}/{}/{}/{}'.format(ripe_base_url, apnic_sub, year, filename)

	sys.stdout.write('Fetching APNIC data from {} ... '.format(apnic_url))
	sys.stdout.flush()

	tmpfile = '{}/apnic-{:04d}{:02d}{:02d}.gz'.format(tmpdir, day.year, day.month, day.day)

	try:
		urllib.request.urlretrieve(apnic_url, tmpfile)
	except:
		raise Exception('Failed to download {} to {}'.format(apnic_url, tmpfile))

	print('OK ({})'.format(tmpfile))

	return tmpfile

# Retrieve NRO stats for the LACNIC region for
# the specified date
def fetch_lacnic_nrostats(day):
	# Where do the archives at the RIPE NCC
	start_date = datetime.date(2004,1,1)

	if day < start_date:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/lacnic-empty'.format(tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	# The archive at the RIPE NCC misses data for 
	# certain dates which we replace with newer
	# data
	missing_dateranges = []
	missing_dateranges.append((datetime.date(2018,9,26), datetime.date(2018,9,26), datetime.date(2018,9,27)))
	missing_dateranges.append((datetime.date(2018,11,10), datetime.date(2018,11,10), datetime.date(2018,11,11)))
	missing_dateranges.append((datetime.date(2019,12,21), datetime.date(2019,12,21), datetime.date(2019,12,22)))
	missing_dateranges.append((datetime.date(2020,4,22), datetime.date(2020,4,22), datetime.date(2020,4,23)))

	for daterange in missing_dateranges:
		if day >= daterange[0] and day <= daterange[1]:
			print('{} is missing for LACNIC, replacing it by {}'.format(day, daterange[2]))
			day = daterange[2]

	# The LACNIC data should all be in a single
	# directory, without any exceptions
	lacnic_url = '{}/{}/delegated-lacnic-{:04d}{:02d}{:02d}'.format(ripe_base_url, lacnic_sub, day.year, day.month, day.day)

	sys.stdout.write('Fetching LACNIC data from {} ... '.format(lacnic_url))
	sys.stdout.flush()

	tmpfile = '{}/lacnic-{:04d}{:02d}{:02d}'.format(tmpdir, day.year, day.month, day.day)

	try:
		urllib.request.urlretrieve(lacnic_url, tmpfile)
	except:
		raise Exception('Failed to download {} to {}'.format(lacnic_url, tmpfile))

	print('OK ({})'.format(tmpfile))

	return tmpfile

# Retrieve NRO stats for the RIPE region for
# the specified date
def fetch_ripencc_nrostats(day):
	# Where do the archives at the RIPE NCC
	start_date = datetime.date(2003,11,26)

	if day < start_date:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/ripencc-empty'.format(tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	# The RIPE NCC's archives are split up
	# by year with, annoyingly, a single
	# exception for Jan. 1st, 2004
	year = day.year

	if day == datetime.date(2004,1,1):
		year = 2003
		print('RIPE data for {} lives in year {}'.format(day, year))

	ripencc_url = '{}/{}/{}/delegated-ripencc-{:04d}{:02d}{:02d}.bz2'.format(ripe_base_url, ripencc_sub, year, day.year, day.month, day.day)

	sys.stdout.write('Fetching RIPE data from {} ... '.format(ripencc_url))
	sys.stdout.flush()

	tmpfile = '{}/ripencc-{:04d}{:02d}{:02d}.bz2'.format(tmpdir, day.year, day.month, day.day)

	try:
		urllib.request.urlretrieve(ripencc_url, tmpfile)
	except:
		raise Exception('Failed to download {} to {}'.format(ripencc_url, tmpfile))

	print('OK ({})'.format(tmpfile))

	return tmpfile

# Added the specified prefix to the radix tree and check if
# it might already be present
def add_prefix_to_radix(radix, prefix, prefix_info):
	# Find out if the prefix is already there
	rnode = radix.search_exact(prefix)

	if rnode is None:
		rnode = radix.add(prefix)
	else:
		print('Warning: {} already in radix tree'.format(prefix))

	info_arr = rnode.data.get('info', [])
	info_arr.append(prefix_info)
	rnode.data['info'] = info_arr

# Adding an IPv4 address to the radix tree is tricky, since the
# number of IP addresses in the block in the NRO stats is not
# necessarily a power of two. We first test if it is a power of
# 2, and if it is not, we need to factorize it to get the
# subprefixes
def add_v4_block_to_radix(v4radix, prefix_addr, addrcount, prefix_info):
	prefix_addr = ipaddress.ip_address(prefix_addr)

	while addrcount > 0:
		prefix_size = 32 - int(math.log(addrcount, 2))

		prefix_found = False

		while not prefix_found:
			try:
				test_nw = ipaddress.IPv4Network('{}/{}'.format(prefix_addr, prefix_size))
				prefix_found = True
			except ValueError as v:
				prefix_size += 1

		prefix = '{}/{}'.format(prefix_addr, prefix_size)

		used_addrcount = int(math.pow(2, 32 - prefix_size))
		add_prefix_to_radix(v4radix, prefix, prefix_info + (used_addrcount,))

		addrcount -= used_addrcount
		prefix_addr += used_addrcount

# This is a very crude parser that will only process NRO stat lines
# that are for IPv4 or IPv6 prefixes and the contain exactly 7 fields
# separated by a '|' sign.
def parse_nro_stats(fd, v4radix, v6radix):
	for line in fd:
		if type(line) is bytes:
			line = line.decode('utf8')
		line = line.strip('\r').strip('\n')
		fields = line.split('|')

		if len(fields) < 7 or (fields[2] != 'ipv4' and fields[2] != 'ipv6'):
			continue

		# We put the following information about the
		# prefix in the tuple:
		#  - RIR name
		#  - country
		#  - date of modification/assignment
		#  - prefix status
		#  - original starting block
		#  - original block IP count
		prefix_info = (fields[0], fields[1], fields[5], fields[6], fields[3], int(fields[4]))

		prefix_size = 0
		prefix_addr = fields[3]

		if fields[2] == 'ipv4':
			addrcount = int(fields[4])
			add_v4_block_to_radix(v4radix, prefix_addr, addrcount, prefix_info)
		elif fields[2] == 'ipv6':
			# IPv6 entries just specify the prefix
			# size
			prefix_size = int(fields[4])
			prefix = '{}/{}'.format(prefix_addr, prefix_size)
			add_prefix_to_radix(v6radix, prefix, prefix_info)

def open_nro_stats_file(filename):
	if filename.endswith('.gz'):
		return gzip.open(filename, 'r')
	elif filename.endswith('.bz2'):
		return bz2.BZ2File(filename, 'r')
	else:
		return open(filename, 'r')

##
# Main entry point
##

def main():
	if len(sys.argv) != 3:
		print('Usage: merge-nrostats.py <date> <output directory>')
		return

	day = dateutil.parser.parse(sys.argv[1]).date()
	outdir = sys.argv[2]

	v4radix = radix.Radix()
	v6radix = radix.Radix()

	afrinic_file	= None
	arin_file	= None
	apnic_file	= None
	lacnic_file	= None
	ripencc_file	= None

	try:
		afrinic_file	= fetch_afrinic_nrostats(day)
		arin_file	= fetch_arin_nrostats(day)
		apnic_file	= fetch_apnic_nrostats(day)
		lacnic_file	= fetch_lacnic_nrostats(day)
		ripencc_file	= fetch_ripencc_nrostats(day)

		# AfriNIC
		sys.stdout.write('Parsing AfriNIC NRO statistics for {} ... '.format(day))
		sys.stdout.flush()

		afrinic_fd = open_nro_stats_file(afrinic_file)

		parse_nro_stats(afrinic_fd, v4radix, v6radix)

		afrinic_fd.close()

		print('OK')

		# ARIN
		sys.stdout.write('Parsing ARIN NRO statistics for {} ... '.format(day))
		sys.stdout.flush()

		arin_fd = open_nro_stats_file(arin_file)

		parse_nro_stats(arin_fd, v4radix, v6radix)

		arin_fd.close()

		print('OK')

		# APNIC
		sys.stdout.write('Parsing APNIC NRO statistics for {} ... '.format(day))
		sys.stdout.flush()

		apnic_fd = open_nro_stats_file(apnic_file)

		parse_nro_stats(apnic_fd, v4radix, v6radix)

		apnic_fd.close()

		print('OK')

		# LACNIC
		sys.stdout.write('Parsing LACNIC NRO statistics for {} ... '.format(day))
		sys.stdout.flush()

		lacnic_fd = open_nro_stats_file(lacnic_file)

		parse_nro_stats(lacnic_fd, v4radix, v6radix)

		lacnic_fd.close()

		print('OK')

		# RIPE NCC
		sys.stdout.write('Parsing RIPE NCC NRO statistics for {} ... '.format(day))
		sys.stdout.flush()
		
		ripencc_fd = open_nro_stats_file(ripencc_file)

		parse_nro_stats(ripencc_fd, v4radix, v6radix)

		ripencc_fd.close()

		print('OK')

		# Write the consolidated NRO statistics to file
		v4_file = '{}/nrostats-{:04d}{:02d}{:02d}-v4.csv'.format(outdir, day.year, day.month, day.day)
		v6_file = '{}/nrostats-{:04d}{:02d}{:02d}-v6.csv'.format(outdir, day.year, day.month, day.day)

		v4_fd = open(v4_file, 'w')
		v6_fd = open(v6_file, 'w')

		v4_fd.write('prefix,rir,date,country_code,status,block_start,block_ip_count\n')
		v6_fd.write('prefix,rir,date,country_code,status\n')

		sys.stdout.write('Writing IPv4 NRO statistics to {} ... '.format(v4_file))
		sys.stdout.flush()

		for rnode in v4radix:
			prefix = rnode.prefix

			for info in rnode.data['info']:
				rir 		= info[0]
				date 		= info[1]
				country 	= info[2]
				prefix_status	= info[3]
				block_start	= info[4]
				block_ip_count	= info[5]

				v4_fd.write('{},{},{},{},{},{},{}\n'.format(prefix, rir, country, date, prefix_status, block_start, block_ip_count))

		print('OK')

		sys.stdout.write('Writing IPv6 NRO statistics to {} ... '.format(v6_file))
		sys.stdout.flush()

		for rnode in v6radix:
			prefix = rnode.prefix

			for info in rnode.data['info']:
				rir 		= info[0]
				date 		= info[1]
				country 	= info[2]
				prefix_status	= info[3]

				v6_fd.write('{},{},{},{},{}\n'.format(prefix, rir, country, date, prefix_status))

		print('OK')

		v4_fd.close()
		v6_fd.close()
	except Exception as e:
		print('Failed to retrieve and merge NRO statistics ({})'.format(e))
	finally:
		if afrinic_file is not None:
			os.unlink(afrinic_file)
		if arin_file is not None:
			os.unlink(arin_file)
		if apnic_file is not None:
			os.unlink(apnic_file)
		if lacnic_file is not None:
			os.unlink(lacnic_file)
		if ripencc_file is not None:
			os.unlink(ripencc_file)

	return

if __name__ == "__main__":
	main()
