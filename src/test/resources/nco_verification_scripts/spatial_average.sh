#!/usr/bin/env bash

datafile="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA_TEST_DATA.ta.nc"

ncap2 -O -v -S cosine_weights.nco ${datafile} gw.nc

ncwa -O -w gw -d time,"1979-01-16T12:00:00Z","1979-01-16T12:00:00Z" -a lat,lon ${datafile} spatial_average.nc
