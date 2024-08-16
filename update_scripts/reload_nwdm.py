#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares for RWS Waterinfo Extra
#   Gerrit.Hendriksen@deltares.nl
#   Ioanna.Micha@deltares.nl
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
Script is used to import chloride measuruments and for take up in datamodel for timeseries
"""
import os
import pandas as pd
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


def establish_connection(fc, connections_string=None):
    """
    Set up a orm session to the target database with the connectionstring
    in the file that is passed

    Parameters
    ----------
    fc : string
        DESCRIPTION.
        Location of the file with a connectionstring to a PostgreSQL/PostGIS
        database
    connectionstring:
        DESCRIPTION.
        in case fc = none, a connectionstring can be passed

    Returns
    -------
    session : ormsession
        DESCRIPTION.
        returns orm session

    """
    if fc != None:
        f = open(fc)
        engine = create_engine(f.read(), echo=False)
        f.close()
    elif fc == None and connections_string != None:
        engine = create_engine(connections_string, echo=False)

    Session = sessionmaker(bind=engine)
    session = Session()
    session.rollback()
    return session, engine


def read_config(af):
    # Default config file (relative path, does not work on production, weird)
    # Parse and load
    cf = configparser.ConfigParser()
    cf.read(af)
    return cf


# set reference to config file
local = True
if local:
    fc = r"C:\develop\nwdm\configuration.txt"
else:
    fc = r"C:\develop\nwdm_upserts\config_nwdm.txt"

cf = read_config(fc)
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
session, engine = establish_connection(None, connstr)

#PATHS
mapping_table_file = r"C:\develop\nwdm\etl\mapping\mappingRWSNIOZacidity.csv"
data_table_file = (
    r"C:\develop\nwdm\data\RWS-NIOZ North Sea data v2023_10 for SDG14-3-1.xlsx"
)


mapping_table= "mapping_nioz"
data_table = "nioz"
schema = "import"

# delete the current import tables
strsql = f"drop table if exists {schema}.{mapping_table}"
with engine.connect() as conn:
    result = conn.execute(text(strsql))

strsql = f"drop table if exists {schema}.{data_table}"
with engine.connect() as conn:
    result = conn.execute(text(strsql))


# based on the format use differen read function of PANDAS


df_mapping = pd.read_csv(mapping_table_file, sep=';')
df_mapping["_recordnr"] = df_mapping.index + 1
df_mapping["_short_filename"] = "mappingRWSNIOZacidity.csv"
df_mapping["_path"] = "https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg"


df_data = pd.read_excel(data_table_file)
df_data["_recordnr"] = df_data.index + 1
df_data["_short_filename"] = "mappingRWSNIOZacidity.csv"
df_data["_path"] = "https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg"
df_data.columns = df_data.columns.str.lower()

df_data.rename(columns={'date_utc': 'datetime'}, inplace=True)

df_data['datetime'] = pd.to_datetime(df_data['datetime'], errors='coerce')

df_mapping.to_sql(mapping_table, engine, schema=schema)

df_data.to_sql(data_table, engine, schema=schema)


   

