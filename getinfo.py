#!/Users/XXXX-1/.pyenv/shims/python3

from netCDF4 import Dataset
import numpy as np
import sys
import matplotlib.pyplot as plt
#from mpl_toolkits.basemap import Basemap
from pandas import DataFrame
import datetime
import csv

nc =Dataset('Complete_TAVG_Daily_LatLong1_2010.nc')

print(nc.variables.keys())

"""for var in nc.variables.keys():
    data=nc.variables[var][:].data
    print(var,data.shape)"""

land_mask=nc.variables['land_mask'][:].data
print(land_mask.shape)
land_cnt = 0
water_cnt = 0
land_locs = []
water_locs = []
half_locs = []
for i in range(len(land_mask)):
	for j in range(len(land_mask[0])):
		if (land_mask[i, j] == 0.0): 
			water_cnt += 1
			water_locs.append((i, j))
		elif (land_mask[i, j] == 1.0): 
			land_cnt += 1
			land_locs.append((i, j))
		else:
			half_locs.append((i, j))
print(water_cnt, land_cnt)
print(len(half_locs))

temperature=nc.variables['temperature'][:].data
print(temperature.shape)

header = ['time', 'latitude', 'longitude', 'temperature']

csv_data = []

for time in range(3652):
	for (latitude, longitude) in land_locs:
		csv_data.append([time, latitude, longitude, temperature[time, latitude, longitude]])
		#print([time, latitude, longitude, temperature[time, latitude, longitude]])

with open('data.csv', 'w', encoding='UTF8', newline='') as f:
	writer = csv.writer(f)
	writer.writerow(header)
	writer.writerows(csv_data)
