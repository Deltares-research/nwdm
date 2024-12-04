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
from sqlalchemy import text, Table, Column, String, Integer, MetaData, DateTime, Float, insert, select
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

# setting up mapping table
metadata_obj = MetaData(schema="import")

stmt = """drop table if exists import.ices"""
with engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit()

class Base(DeclarativeBase):
    metadata = metadata_obj

class mapping(Base):
    __tablename__ = 'mapping_ices'
    icesid = Column(Integer, primary_key=True)
    icescolumn = Column(String(50))
    p01 = Column(String(10))
    p06 = Column(String(5))
    _short_filename = Column(String(50))

class mapping(Base):
    __tablename__ = 'ices'
    recordnr_dataset = Column(Integer, primary_key=True)
    cruise = Column(String(125))
    station = Column(String(125))
    type = Column(String(125))
    time = Column(String(125))
    lon  = Column(Float)
    lat = Column(Float)
    bot_depth = Column(Float)
    secchi_depth = Column(Float)
    device_cat = Column(Integer)
    platform_code = Column(String(125))
    distributor = Column(Integer)
    custodian = Column(Integer)
    originator = Column(String(125))
    project_code = Column(String(125))
    modified = Column(String(125))
    guid = Column(DateTime)
    adepzz01_ulaa = Column(Float)
    qv_adepzz01 = Column(Float)
    temppr01_upaa = Column(Float)
    qv_temppr01 = Column(Float)
    psalpr01_uuuu = Column(Float)
    qv_psalpr01 = Column(Float)
    doxyzzxx_umll = Column(Float)
    qv_doxyzzxx = Column(Float)
    phoszzxx_upox = Column(Float)
    qv_phoszzxx = Column(Float)
    slcazzxx_upox = Column(Float)
    qv_slcazzxx = Column(Float)
    ntrazzxx_upox = Column(Float)
    qv_ntrazzx = Column(Float)
    phxxzzxx_uuph = Column(Float)
    qv_phxxzzxx = Column(Float)
    chplzzxx_ugpl = Column(Float)
    qv_cphllzzxx = Column(Float)
    cndczz01_ueca = Column(Float)
    qv_cndczz01 = Column(Float)

Base.metadata.create_all(engine)

# connect to existing table with 
META_DATA = MetaData(schema='nwdm')
META_DATA.reflect(engine)
parameter = META_DATA.tables['nwdm.parameter']
unit = META_DATA.tables['nwdm.unit']
quality = META_DATA.tables['nwdm.quality']


# reading 21 Gb csv, needs be done chunck wise and column wise
header = 12 # lines 
chunksize = 10 ** 6

thecsv = r'C:\temp\nwdm\ICES_StationSamples_CTD_2024-07-10.csv'
# only select data within 
# "BOX(-15.304392 42.875958,13.278998 64.098841)"
# via the sqldf package the subsetting is set up, then store the data in temp data part
with pd.read_csv(thecsv, sep=',',header=12, dtype={1:'str'},chunksize=chunksize) as reader:
    for chunk in reader:
        chunk.columns=['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01']
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.to_sql('ices', engine,schema='import',if_exists='append',index=False)


# insert into mapping table
lstparamters = ['adepzz01_ulaa',
                'temppr01_upaa',
                'psalpr01_uuuu',
                'doxyzzxx_umll',
                'phoszzxx_upox',
                'slcazzxx_upox',
                'ntrazzxx_upox',
                'phxxzzxx_uuph',
                'cphlzzxx_ugpl',
                'cndczz01_ueca']
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

## 0. update data_owners
strsql = """insert into nwdm.data_owner (data_owner,priority) values ('ICES',11) on conflict do nothing;"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

## 1. update administration of source - dataset id
strsql = """insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 700000 dataset_id
, 'ICES_StationSamples_CTD_2024-07-10.csv' as dataset_name
, 'null' as short_filename
, 'null' as "_path"
, 'null' as file
,  null number_of_records
, 'ICES' as data_holder
, 'ICES' as data_owner
, 'https://www.ices.dk/data' as link_to_data
, 'https://gis.ices.dk/geonetwork/srv/eng/catalog.search#/search?facet.q=type%2Fdataset%26orgName%2FICES' as link_to_metadata"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

strsql = """update nwdm.dataset set dataset_name = 'ICES_StationSamples_CTD_2024-07-10.csv' 
            where data_holder = 'ICES' and data_owner = 'ICES' """
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

### update 11. ices locations
strsql = """delete from nwdm.location where data_owner='ICES';"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

strsql = """insert into nwdm.location(location_code, location_name,x,y,epsg,geom,number_of_observations,first_year, last_year,data_owner)
select *
from (
    select
     'ices_'||cruise||'_'||(row_number() over ())::text as location_code  -- check if this needs to be a unique number
    , coalesce(station, 'station'||cruise::varchar) as location_name
    , (rd.lon)::decimal as x
    , (rd.lat)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(lon,lat), 4326::int) as geom
	, rd.noobs as number_of_observations
	, rd.startyear as startyear
	, rd.endyear as lastyear
    , 'ICES' data_owner
    from (	select distinct cruise, station, lat, lon, count(*) as noobs, min(extract(year from time::timestamp)) as startyear, max(extract(year from time::timestamp)) as endyear
			from import.ices
			group by cruise, station, lat, lon
    ) rd
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

# update import.ices and add unique key that is used as recordnr_dataset
strsql = """alter table import.ices add column recordnr_dataset serial"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit() 

# update per parameter the measurument table
stmt = """delete from nwdm.measurement where dataset_id = 700000"""
with engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit() 

# the dataset is not consequent with respect to time notation
# sometimes there are times like "1993-03-22T15Z" where it should at least be "1993-03-22T15:00Z"

datasetid = 700000
for param in lstparamters:
    decparameter = param.split('_')[0].upper()
    decunit = param.split('_')[1].upper()
    idparam = session.query(parameter).filter_by(code=decparameter).first()
    idunit  = session.query(unit).filter_by(code=decunit).first()
    qualityclmn = 'qv_'+decparameter.lower()
    stmt = f"""insert into nwdm.measurement (
                recordnr_dataset, 
                dataset_id, 
                 
                "depth", 
                vertical_reference_id, 
                parameter_id, 
                unit_id, 
                value, 
                quality_id,
                geom,
                "date")
              select 
                recordnr_dataset, 
                {datasetid},
                bot_depth,
                4,
                {idparam.parameter_id},
                {idunit.unit_id},
                {param},
                qv.quality_id,
                st_setsrid(st_point(lon,lat),4326),
                case
                    when length(time) = 10 then (time||'T00:00Z')::timestamp
                    when length(time) = 14 then replace(time,'Z','00Z')::timestamp
                    else time::timestamp
                end
                from import.ices
                join nwdm.quality qv on qv.code = {qualityclmn}::text 
                where {param} is not null"""
    with engine.connect() as conn:
        conn.execute(text(stmt))
        conn.commit()