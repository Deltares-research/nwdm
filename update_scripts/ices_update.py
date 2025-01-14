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
#%%
"""
Load and pre-process the data from ICES 
"""
import os
import sys  
import pandas as pd
from pandasql import sqldf
from sqlalchemy import text, Table, Column, String, Integer, MetaData, DateTime, Float, insert, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import DeclarativeBase
import re

# add the path to the sys.path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import utils

localdev = False
if localdev:
    cf_file = r"C:\develop\nwdm\configuration.txt"
    #cf_file = r'C:\projecten\temp\nwdm\nwdm_local.txt'
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
    device_cat = Column(String(125))
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

#check columns!
class icesdata_bot(Base):
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
    temppr01_upaa= Column(Float) 
    qv_temppr01= Column(Float) 
    psalpr01_uuuu= Column(Float) 
    qv_psalpr01= Column(Float)
    doxyzzxx_umll= Column(Float) 
    qv_doxyzzxx= Column(Float) 
    phoszzxx_upox= Column(Float) 
    qv_phoszzxx= Column(Float)
    tphszzxx_upox= Column(Float) 
    qv_tphszzxx= Column(Float) 
    slcazzxx_upox= Column(Float) 
    qv_slcazzxx= Column(Float)
    ntrzzzxx_upox= Column(Float) 
    qv_ntrzzzxx= Column(Float) 
    ntrazzxx_upox= Column(Float) 
    qv_ntrazzxx= Column(Float)
    ntrizzxx_upox= Column(Float) 
    qv_ntrizzxx= Column(Float) 
    amonzzxx_upox= Column(Float) 
    qv_amonzzxx= Column(Float)
    ntotzzxx_upox= Column(Float) 
    qv_ntotzzxx= Column(Float) 
    h2sxzzxx_upox= Column(Float) 
    qv_h2sxzzxx= Column(Float)
    phxxzzxx_uuph= Column(Float) 
    qv_phxxzzxx= Column(Float) 
    alkyzzxx_meql= Column(Float) 
    qv_alkyzzxx= Column(Float)
    cphlzzxx_ugpl= Column(Float) 
    qv_cphlzzxx= Column(Float) 
    turbxxxx_ustu= Column(Float) 
    qv_turbxxxx= Column(Float)
    phtxpr01_upaa= Column(Float) 
    qv_phtxpr01= Column(Float) 
    corgzzzx_upox= Column(Float) 
    qv_corgzzzx= Column(Float) 
# first check if it necessary if you need to create the tables
if localdev:
    # Base.metadata.drop_all(engine)
    # Base.metadata.create_all(engine)
else:
    print('else')
    # Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # reading 21 Gb csv, needs be done chunck wise and column wise
header = 12 # lines 
chunksize = 10 ** 6

#%%
# connect to existing table with 
META_DATA = MetaData(schema='nwdm')
META_DATA.reflect(engine)
dataset = META_DATA.tables['nwdm.dataset']
parameter = META_DATA.tables['nwdm.parameter']
unit = META_DATA.tables['nwdm.unit']
quality = META_DATA.tables['nwdm.quality']


# function that maps parameters agains P01
def maplstparameters(lstparameters):
    i = 0
    for c in lstparameters:
        i=i+1
        # first check if specific mapping already exists
        r = session.query(mapping).filter_by(icescolumn=c).first()
        msg = str(r)
        try:
            if str(r) == "None":
                maxidx = session.query(func.max(mapping.icesid)).first() 
                stmt = insert(mapping).values(icesid=maxidx[0]+1,
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


# # dropt the table if exists
# stmt = """drop table if exists import.ices"""
# with engine.connect() as conn:
#     conn.execute(text(stmt))
#     conn.commit()

# several csvs, for all these csv's different header sizes are used
# dict is list of csvs incl. 
# values of the dict are:
# thecsv = full path to csv
# hdr = the header size ( // sign is headersize that needs to be taken into account)
# cols = list of colums (total list of colums in the csv, mapped to something readable (and equal to table definition above))
# lstparams = list of parameters that need to be mapped with BODC list, (P01)
dctcsv = {}
dctcsv['CTD'] = [r'C:\temp\nwdm\ICES_StationSamples_CTD_2024-07-10.csv',12, ['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzx','phxxzzxx_uuph','qv_phxxzzxx','chplzzxx_ugpl','qv_cphllzzxx','cndczz01_ueca','qv_condczz01'],['adepzz01_ulaa','temppr01_upaa','psalpr01_uuuu','doxyzzxx_umll','phoszzxx_upox','slcazzxx_upox','ntrazzxx_upox','phxxzzxx_uuph','cphlzzxx_ugpl']]
dctcsv['BOT'] = [r"C:\temp\nwdm\ICES_StationSamples_BOT_2024-07-10.csv",21, ['cruise','station','type','time','lon','lat','bot_depth','secchi_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','tphszzxx_upox','qv_tphszzxx','slcazzxx_upox','qv_slcazzxx','ntrzzzxx_upox','qv_ntrzzzxx','ntrazzxx_upox','qv_ntrazzxx','ntrizzxx_upox','qv_ntrizzxx','amonzzxx_upox','qv_amonzzxx','ntotzzxx_upox','qv_ntotzzxx','h2sxzzxx_upox','qv_h2sxzzxx','phxxzzxx_uuph','qv_phxxzzxx','alkyzzxx_meql','qv_alkyzzxx','cphlzzxx_ugpl','qv_cphlzzxx','turbxxxx_ustu','qv_turbxxxx','phtxpr01_upaa','qv_phtxpr01','corgzzzx_upox','qv_corgzzzx'],['adepzz01_ulaa','temppr01_upaa','psalpr01_uuuu','doxyzzxx_umll','phoszzxx_upox','tphszzxx_upox','slcazzxx_upox','ntrzzzxx_upox','ntrazzxx_upox','ntrizzxx_upox','amonzzxx_upox','ntotzzxx_upox','h2sxzzxx_upox','phxxzzxx_uuph','alkyzzxx_meql','cphlzzxx_ugpl','turbxxxx_ustu','phtxpr01_upaa','corgzzzx_upox']]
dctcsv['PMP'] = [r"C:\temp\nwdm\ICES_StationSamples_PMP_2024-07-10.csv",16, ['cruise','station','time','lon','lat','bot_depth','device_cat','platform_code','distributor','custodian','originator','project_code','modified','guid','adepzz01_ulaa','qv_adepzz01','temppr01_upaa','qv_temppr01','psalpr01_uuuu','qv_psalpr01','doxyzzxx_umll','qv_doxyzzxx','phoszzxx_upox','qv_phoszzxx','tphszzxx_upox','qv_tphszzxx','slcazzxx_upox','qv_slcazzxx','ntrazzxx_upox','qv_ntrazzxx','ntrizzxx_upox','qv_ntrizzxx','amonzzxx_upox','qv_amonzzxx','ntotzzxx_upox','qv_ntotzzxx','phxxzzxx_uuph','qv_phxxzzxx','alkyzzxx_meql','qv_alkyzzxx','cphlzzxx_ugpl','qv_cphlzzxx'],['adepzz01_ulaa','temppr01_upaa','psalpr01_uuuu','doxyzzxx_umll','phoszzxx_upox','tphszzxx_upox','slcazzxx_upox','ntrazzxx_upox','ntrizzxx_upox','amonzzxx_upox','ntotzzxx_upox','phxxzzxx_uuph','alkyzzxx_meql','cphlzzxx_ugpl']]


# only select data within 
# "BOX(-15.304392 42.875958,13.278998 64.098841)"
# via the sqldf package the subsetting is set up, then store the data in temp data part
for param in dctcsv.keys():
    thecsv  = dctcsv[param][0]
    hdr     = dctcsv[param][1]
    cols    = dctcsv[param][2]
    # update the parameter mapping_ices table against P01
    lstprms = dctcsv[param][3]
    maplstparameters(lstprms)
    # load the data in the database.
    with pd.read_csv(thecsv, sep=',',header=hdr, dtype={1:'str'},chunksize=chunksize) as reader:
        for chunk in reader:
            chunk.columns=cols
            chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
            chns.to_sql('_'.join(['ices',param.lower()]), engine,schema='import',if_exists='append',index=False)


#%%  
## 0. update data_owners
strsql = """insert into nwdm.data_owner (data_owner,priority) values ('ICES',11) on conflict do nothing;"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

## 1. update administration of source - dataset id
def administratefile(lsttbls,basedsid):
    """File administration in the datamodel. Baseid is an arbitrary number

    Args:
        lsttbl (list): list of table names (in the routine the basename, so only the filename, without path will be extracted and used)
        baseid (interger): arbitrary number (in case of ICES data 700000)
    """
    for tbl in lsttbls:
        print('registring file', os.path.basename(tbl))
        r = session.query(dataset).filter_by(dataset_name=os.path.basename(tbl)).first()
        try:
            if str(r) == 'None':
                basedsid += 1
                stmt = insert(dataset).values(dataset_id = basedsid,
                            dataset_name = os.path.basename(tbl),
                            data_holder = 'ICES',
                            data_owner = 'ICES',
                            link_to_data = 'https://www.ices.dk/data',
                            link_to_metadata = 'https://gis.ices.dk/geonetwork/srv/eng/catalog.search#/search?facet.q=type%2Fdataset%26orgName%2FICES'
                )
                with engine.connect() as conn:
                                    conn.execute(stmt)
                                    conn.commit()
                msg = ('added ices column '+ os.path.basename(tbl)+ ' to dataset table with number', basedsid)
            else:
                msg = (os.path.basename(tbl)+ ' already registered with id', r[0])
        except:
            msg("exception raised while retrieving/assigning item to mapping table, item is ")
        finally:
            print(msg)

# the data is ordered in a dictionary that has a list of parameters for each file that needed to be loaded
# construct a list of files from the dictionary
baseid = 700000
lsttbls = []
for ds in dctcsv.keys():
    tbl = dctcsv[ds][0]
    lsttbls.append(tbl)

administratefile(lsttbls,baseid)

### update 11. ices locations
# following query only necessary if you need to delete all locations.
strsql = """delete from nwdm.location where data_owner='ICES';"""
with engine.connect() as conn:
    conn.execute(text(strsql))
    conn.commit()    

for entry in dctcsv.keys():
    tbl = '_'.join(['ices',entry.lower()])
    print(tbl) 
    strsql = f"""insert into nwdm.location(location_code, location_name,x,y,epsg,geom,number_of_observations,first_year, last_year,data_owner)
    select *
    from (
        select
        '{tbl}'||'_'||cruise||'_'||station||'_'||time::text||'_'||lat::text||'_'||lon::text as location_code
        , coalesce(station, 'station'||cruise::varchar) as location_name
        , (rd.lon)::decimal as x
        , (rd.lat)::decimal as y
        , 4326::int as epsg
        , st_setsrid(st_makepoint(lon,lat), 4326::int) as geom
        , rd.noobs as number_of_observations
        , rd.startyear as startyear
        , rd.endyear as lastyear
        , 'ICES' data_owner
        from (	select distinct cruise, station, time, lat, lon, count(*) as noobs, min(left(time,4)::int) as startyear, max(left(time,4)::int) as endyear
                from import.{tbl}
                group by cruise, station, lat, lon, time
        ) rd
    ) g where st_contains((select geom from nwdm.scope_northsea),g.geom);"""
    with engine.connect() as conn:
        conn.execute(text(strsql))
        conn.commit()

# line cruise||'_'||station||'_'||time::text||'_'||lat::text||'_'||lon::text as location_code
# used as alternative for 'ices_'||cruise||'_'||(row_number() over ())::text as location_code  -- check if this needs to be a unique number
# it enables finding the correct location in following steps.

# update per parameter the measurument table ## optional query, mostly used during testing
stmt = """delete from nwdm.measurement where dataset_id = 700000"""
with engine.connect() as conn:
    conn.execute(text(stmt))
    conn.commit() 


#set some indices on colums

# the dataset is not consequent with respect to time notation
# sometimes there are times like "1993-03-22T15Z" where it should at least be "1993-03-22T15:00Z"
# hence the case part in the query where the notation is harmonized
for typobs in dctcsv.keys():
    thecsv = os.path.basename(dctcsv[typobs][0])
    datasetid = session.query(dataset).filter_by(dataset_name=thecsv).first()[0]
    lstprms = dctcsv[typobs][3]
    imptable = '_'.join(['ices',typobs.lower()])
    print(typobs, 'has parameters',lstprms)
    for param in lstprms:
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
                        qv.quality_id,
                        st_setsrid(st_point(lon,lat),4326),
                        case
                            when length(time) = 10 then (time||'T00:00Z')::timestamp
                            when length(time) = 14 then replace(time,'Z','00Z')::timestamp
                            else time::timestamp
                        end
                        from import.{imptable} imp
                        join nwdm.vertical_reference vr on vr.code = 'D08'
                        join nwdm.quality qv on qv.code = imp.{qualityclmn}::text 
                        join nwdm.quality qvd on qvd.code = imp.qv_adepzz01::text
                        join nwdm.location l on l.location_code = '{imptable}'||'_'||imp.cruise||'_'||imp.station||'_'||imp.time::text||'_'||imp.lat::text||'_'||imp.lon::text
                        where {param} is not null and imp.{qualityclmn} is not null"""
            with engine.connect() as conn:
                conn.execute(text(stmt))
                conn.commit()
            print('converted data for column ', param, ' to nwdm.measurements')    
            session.close()
            engine.dispose()

# after this initial loading, the sqls in nabewerking.sql should be executed.

# %%
 ## repetative for PMP

 # Function to extract and transform column names
def rename_columns(col_name):
    if col_name.startswith('QV:ODV:'):
        match = re.search(r'\((.*?)\)', col_name)
        if match:
            extracted = match.group(1).lower()
            extracted = extracted.split('_')[0]  # Remove the part after the underscore
            return 'qv_' + extracted
    else:
        match = re.search(r'\((.*?)\)', col_name)
        if match:
            return match.group(1).lower()
    return col_name

thecsv = r"C:\projecten\temp\nwdm\ICES_StationSamples_PMP_2024-07-10.csv"
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

mapping_table = pd.read_excel(r"C:\projecten\temp\nwdm\datacolumns_2.xlsx", sheet_name = 'mapping_table')
# Convert the mapping DataFrame to a dictionary
mapping_table = pd.Series(mapping_table.New_name.values, index=mapping_table.Old_name).to_dict()

# read pmp csv table , check columsn in the chunk.columns
with pd.read_csv(thecsv, sep=',',header=16, dtype={1:'str'},chunksize=chunksize) as reader:
    first_chunk = next(reader)
    # Use the first chunk for mapping and upload the first chunk
    mapped_headers = [mapping_table.get(col, col) for col in first_chunk.columns]
    # Process the first chunk with the new headers
    first_chunk.columns = mapped_headers
    first_chunk.rename(columns={col: rename_columns(col) for col in first_chunk.columns}, inplace=True)
    chns = sqldf('''select * from first_chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
    chns.to_sql('ices_pmp', engine,schema='import',if_exists='append',index=False)
    print('uploaded first chunk')
    for chunk in reader:
        chunk.columns= mapped_headers #use the mapped headers for the next chunk
        chunk.rename(columns={col: rename_columns(col) for col in chunk.columns}, inplace=True)
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.to_sql('ices_pmp', engine,schema='import',if_exists='append',index=False)
        print('uploaded chunk')

print('finish pmp')

#%% BOT

thecsv = r"C:\projecten\temp\nwdm\ICES_StationSamples_BOT_2024-07-10.csv"

mapping_table = pd.read_excel(r"C:\projecten\temp\nwdm\datacolumns_2.xlsx", sheet_name = 'mapping_table')
# Convert the mapping DataFrame to a dictionary
mapping_table = pd.Series(mapping_table.New_name.values, index=mapping_table.Old_name).to_dict()

# read pmp csv table , check columsn in the chunk.columns
with pd.read_csv(thecsv, sep=',',header=21, dtype={1:'str'},chunksize=chunksize) as reader:
    first_chunk = next(reader)
    # Map the headers
    mapped_headers = [mapping_table.get(col, col) for col in first_chunk.columns]
    # Process the first chunk with the new headers
    first_chunk.columns = mapped_headers
    first_chunk.rename(columns={col: rename_columns(col) for col in first_chunk.columns}, inplace=True)
    first_chunk.drop(columns={'Secchi Depth [m]'}, inplace=True)
    chns = sqldf('''select * from first_chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
    chns.to_sql('ices_bot', engine,schema='import',if_exists='append',index=False)
    print('uploaded first chunk')

    for chunk in reader:
        #rest of the chunks automatically
        chunk.columns = mapped_headers
        chunk.drop(columns={'Secchi Depth [m]'}, inplace=True)
        chunk.rename(columns={col: rename_columns(col) for col in chunk.columns}, inplace=True)
        chns = sqldf('''select * from chunk where lon > -15 and lon < 13.3 and lat > 42.8 and lat < 64.1''')
        chns.to_sql('ices_bot', engine,schema='import',if_exists='append',index=False)
        print('uploaded chunk')

print('finish bot')

# %%
