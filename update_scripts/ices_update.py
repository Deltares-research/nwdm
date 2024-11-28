# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2022 Deltares
#       Gerrit Hendriksen
#       Gerrit Hendriksen@deltares.nl
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.
"""
Load and pre-process the data from ICES 
"""
import os
import sys  
import pandas as pd
from sqlalchemy import text

# add the path to the sys.path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import utils


cf_file = r"C:\develop\nwdm\configuration.txt"
cf = utils.read_config(cf_file)
connstr = (
    "postgresql+psycopg2://"
    + cf.get("Postgis", "user")
    + ":"
    + cf.get("Postgis", "pwd")
    + "@"
    + cf.get("Postgis", "host")
    + ":5432/"
    + cf.get("Postgis", "dbname")
)
session, engine = utils.establish_connection(None, connstr)


# reading 21 Gb csv, needs be done chunck wise and column wise
header = 12 # lines 
chunksize = 10 ** 6
# base usecolls
usecols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]

thecsv = r'c:\develop\nwdm\data\ices\ICES_StationSamples_CTD_2024-07-10.csv'

with pd.read_csv(thecsv, sep=',',header=12, chunksize=chunksize) as reader:
    for chunk in reader:
        chunk.columns=['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01']
        chunk.to_sql('ices', engine,schema='nwdm',if_exists='append',index=False)