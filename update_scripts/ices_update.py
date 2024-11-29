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
from pandasql import sqldf
from sqlalchemy import text, Table, Column, String, Integer, MetaData, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeBase

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

thecsv = r'C:\temp\nwdm\ICES_StationSamples_CTD_2024-07-10.csv'
dtypes = []
# only select data within 
# "BOX(-15.304392 42.875958,13.278998 64.098841)"
# via the sqldf package the subsetting is set up
with pd.read_csv(thecsv, sep=',',header=12, dtype={1:'str'},chunksize=chunksize) as reader:
    for chunk in reader:
        chunk.columns=['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01']
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.astype({'station': 'str'}).dtypes
        chns.to_sql('ices', engine,schema='import',if_exists='append',index=False)

# setting up mapping table
metadata_obj = MetaData(schema="import")

class Base(DeclarativeBase):
    metadata = metadata_obj

class mapping(Base):
    __tablename__ = 'mapping_ices'
    icesid = Column(Integer, primary_key=True)
    icescolumn = Column(String(50))
    p01 = Column(String(10))
    p06 = Column(String(5))
    _short_filename = Column(String(50))


Base.metadata.create_all(engine)

# connect to existing table with 
META_DATA = MetaData(bind=engine, reflect=True)
parameter = META_DATA.tables['parameter']
unit = META_DATA.tables['unit']
quality = META_DATA.tables['quality']

lstparamters = ['adepzz01_ulaa','temppr01_upaa','psalpr01_uuuu','doxyzzxx_umll','phoszzxx_upox','slcazzxx_upox','ntrazzxx_upox','phxxzzxx_uuph','chplzzxx_ugpl','cndczz01_ueca']
i = 0
for c in lstparamters:
    i=i+1
    stmt = insert(mapping).values(icesid=i,
                                  icescolumn=c,
                                  p01=c.split('_')[0].upper(),
                                  p06=c.split('_')[1].upper(),
                                  _short_filename=os.path.basename(thecsv))
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
