# -*- coding: utf-8 -*-
# Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares
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

localdev = True
if localdev:
    cf_file = r"C:\develop\nwdm\configuration.txt"
else:
    cf_file = r"C:\develop\nwdm\configuration_nwdm.txt"

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

class Base(DeclarativeBase):
    metadata = metadata_obj

class mapping(Base):
    __tablename__ = 'mapping_ices'
    icesid = Column(Integer, primary_key=True)
    icescolumn = Column(String(50))
    p01 = Column(String(10))
    p06 = Column(String(5))
    p35 = Column(String(5))
    _short_filename = Column(String(50))

class icesdata(Base):
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
    qv_ntrazzxx = Column(Float)
    phxxzzxx_uuph = Column(Float)
    qv_phxxzzxx = Column(Float)
    cphlzzxx_ugpl = Column(Float)
    qv_cphlzzxx = Column(Float)
    cndczz01_ueca = Column(Float)
    qv_cndczz01 = Column(Float)

# first check if it necessary if you need to create the tables
if localdev:
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)
else:
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)

# connect to existing table with 
META_DATA = MetaData(schema='nwdm')
META_DATA.reflect(engine)
dataset = META_DATA.tables['nwdm.dataset']
parameter = META_DATA.tables['nwdm.parameter']
unit = META_DATA.tables['nwdm.unit']
quality = META_DATA.tables['nwdm.quality']


# reading 21 Gb csv, needs be done chunck wise and column wise
header = 12 # lines 
chunksize = 10 ** 6

# dropt the table if exists
stmt = """drop table if exists import.ices"""
with engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit()

# several csv
lstfiles = [r'C:\temp\nwdm\ICES_StationSamples_CTD_2024-07-10.csv',
            r"C:\temp\nwdm\ICES_StationSamples_BOT_2024-07-10.csv",
            r"C:\temp\nwdm\ICES_StationSamples_PMP_2024-07-10.csv"]

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
                'cphlzzxx_ugpl']

def maplstparameters(lstparameters):
    i = 0
    for c in lstparamters:
        i=i+1
        # first check if specific mapping already exists
        r = session.query(mapping).filter_by(icescolumn=c).first()
        msg = str(r)
        try:
            if str(r) == "None":
                stmt = insert(mapping).values(icesid=i,
                                            icescolumn=c,
                                            p01=c.split('_')[0].upper(),
                                            p06=c.split('_')[1].upper(),
                                            _short_filename=os.path.basename(thecsv))
                with engine.connect() as conn:
                    conn.execute(stmt)
                    conn.commit()
                msg = ('added ices column '+ c+ ' to mapping table')
        except:
            msg("exception raised while retrieving/assigning item to mapping table, item is "+c)
        finally:
            print(msg)

    
## 0. update data_owners
strsql = """insert into nwdm.data_owner (data_owner,priority) values ('ICES',11) on conflict do nothing;"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

## 1. update administration of source - dataset id
def administratefile(lstfiles):
    basedsid = 700000  
    for tbl in lstfiles:
        r = session.query(dataset).filter_by(dataset_name=os.path.basename(tbl)).first()
        try:
            if str(r) == 'None':
                stmt = insert(dataset).values(
                            datasetid = basedsid += 1,
                            dataset_name = os.path.basename(tbl),
                            data_holder = 'ICES',
                            data_owner = 'ICES',
                            link_to_data = 'https://www.ices.dk/data',
                            link_to_metadata = 'https://gis.ices.dk/geonetwork/srv/eng/catalog.search#/search?facet.q=type%2Fdataset%26orgName%2FICES'
                )
                with engine.connect() as conn:
                                    conn.execute(stmt)
                                    conn.commit()
            msg = ('added ices column '+ os.path.basename(tbl)+ ' to dataset table')
        except:
            msg("exception raised while retrieving/assigning item to mapping table, item is "+c)
        finally:
            print(msg)            


    # datasetid = basedsid+i
    # strsql = """insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
    # select 700000 dataset_id
    # , 'ICES_StationSamples_CTD_2024-07-10.csv' as dataset_name
    # , 'null' as short_filename
    # , 'null' as "_path"
    # , 'null' as file
    # ,  null number_of_records
    # , 'ICES' as data_holder
    # , 'ICES' as data_owner
    # , 'https://www.ices.dk/data' as link_to_data
    # , 'https://gis.ices.dk/geonetwork/srv/eng/catalog.search#/search?facet.q=type%2Fdataset%26orgName%2FICES' as link_to_metadata"""
    # with engine.connect() as conn:
    #     conn.execute(text(strsql))
    #     conn.commit()    

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
    from (	select distinct cruise, station, lat, lon, count(*) as noobs, min(left(time,4)::int) as startyear, max(left(time,4)::int) as endyear
			from import.ices
			group by cruise, station, lat, lon
    ) rd
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

# update per parameter the measurument table
stmt = """delete from nwdm.measurement where dataset_id = 700000"""
with engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit() 


#set some indices on colums

# the dataset is not consequent with respect to time notation
# sometimes there are times like "1993-03-22T15Z" where it should at least be "1993-03-22T15:00Z"

datasetid = 700000
for param in lstparamters:
    if param != 'adepzz01_ulaa':
        session, engine = utils.establish_connection(None, connstr)
        decparameter = param.split('_')[0].upper()
        decunit = param.split('_')[1].upper()
        idparam = session.query(parameter).filter_by(code=decparameter).first()
        idunit  = session.query(unit).filter_by(code=decunit).first()
        qualityclmn = 'qv_'+decparameter.lower()
        stmt = f"""insert into nwdm.measurement (recordnr_dataset, 
                    location_id,
                    dataset_id,                  
                    "depth", 
                    depth_quality_id,
                    vertical_reference_id, 
                    parameter_id, 
                    unit_id, 
                    value, 
                    quality_id,
                    geom,
                    "date")
                select 
                    imp.recordnr_dataset, 
                    l.location_id,
                    {datasetid},
                    imp.adepzz01_ulaa,
                    qvd.quality_id,
                    vr.vertical_reference_id,
                    {idparam.parameter_id},
                    {idunit.unit_id},
                    {param}::numeric,
                    {qualityclmn}::numeric,
                    st_setsrid(st_point(lon,lat),4326),
                    case
                        when length(time) = 10 then (time||'T00:00Z')::timestamp
                        when length(time) = 14 then replace(time,'Z','00Z')::timestamp
                        else time::timestamp
                    end
                    from import.ices imp
                    join nwdm.vertical_reference vr on vr.code = 'D08'
                    join nwdm.quality qv on qv.code = {qualityclmn}::text 
                    join nwdm.quality qvd on qvd.code = imp.qv_adepzz01::text
                    join nwdm.location l on l.location_name = coalesce(imp.station, 'station'||imp.cruise::varchar) 
                                                            and l.x = imp.lon::decimal and l.y = imp.lat::decimal 
                    where {param} is not null"""
        with engine.connect() as conn:
            conn.execute(text(stmt))
            conn.commit()

        print('converted data for column ', param, ' to nwdm.measurements')    
        session.close()
        engine.dispose()

 ## repetative for PMP
 thecsv = r"C:\temp\nwdm\ICES_StationSamples_PMP_2024-07-10.csv"
 lstparameter = ['adepzz01_ulaa',
                'temppr01_upaa',
                'psalpr01_uuuu',
                'doxyzzxx_umll',
                'phoszzxx_upox',
                'tphszzxx_upox',
                'slcazzxx_upox',
                'ntrazzxx_upox',
                'ntrizzxx_upox',
                'amonzzxx_upox',
                'ntotzzxx_upox',
                'phxxzzxx_uuph',
                'alkyzzxx_meql',
                'cphlzzxx_ugpl']
 
 class icesdata_pmp(Base):
    __tablename__ = 'ices_pmp'
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
    tphszzxx_upox = Column(Float)
    qv_tphszzxx = Column(Float)
    slcazzxx_upox = Column(Float)
    qv_slcazzxx = Column(Float)
    ntrazzxx_upox = Column(Float)
    qv_ntrazzxx = Column(Float)
    ntrizzxx_upox = Column(Float)
    qv_ntrizzxx = Column(Float)
    amonzzxx_upox = Column(Float)
    qv_amonzzxx = Column(Float)
    ntotzzxx_upox = Column(Float)
    qv_ntotzzxx = Column(Float)
    phxxzzxx_uuph = Column(Float)
    qv_phxxzzxx = Column(Float)
    alkyzzxx_meql = Column(Float)
    qv_alkyzzxx = Column(Float)
    cphlzzxx_ugpl = Column(Float)
    qv_cphlzzxx = Column(Float)

# read pmp csv table , check columsn in the chunk.columns
with pd.read_csv(thecsv, sep=',',header=12, dtype={1:'str'},chunksize=chunksize) as reader:
    for chunk in reader:
        chunk.columns=['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01']
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.to_sql('ices', engine,schema='import',if_exists='append',index=False)



 class icesdata_pmp(Base):
    __tablename__ = 'ices_bot'
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
# check rest of the columns
    temppr01_upaa = Column(Float)
    qv_temppr01 = Column(Float)
    psalpr01_uuuu = Column(Float)
    qv_psalpr01 = Column(Float)
    doxyzzxx_umll = Column(Float) 
    qv_doxyzzxx = Column(Float)
    phoszzxx_upox = Column(Float)
    qv_phoszzxx = Column(Float)
    tphszzxx_upox = Column(Float)
    qv_tphszzxx = Column(Float)
    slcazzxx_upox = Column(Float)
    qv_slcazzxx = Column(Float)
    ntrazzxx_upox = Column(Float)
    qv_ntrazzxx = Column(Float)
    ntrizzxx_upox = Column(Float)
    qv_ntrizzxx = Column(Float)
    amonzzxx_upox = Column(Float)
    qv_amonzzxx = Column(Float)
    ntotzzxx_upox = Column(Float)
    qv_ntotzzxx = Column(Float)
    phxxzzxx_uuph = Column(Float)
    qv_phxxzzxx = Column(Float)
    alkyzzxx_meql = Column(Float)
    qv_alkyzzxx = Column(Float)
    cphlzzxx_ugpl = Column(Float)
    qv_cphlzzxx = Column(Float)

# read bot table , check columsn in the chunk.columns
with pd.read_csv(thecsv, sep=',',header=12, dtype={1:'str'},chunksize=chunksize) as reader:
    for chunk in reader:
        chunk.columns=['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01']
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.to_sql('ices', engine,schema='import',if_exists='append',index=False)
