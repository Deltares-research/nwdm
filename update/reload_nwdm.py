#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2024 Deltares for RWS Waterinfo Extra
#   Gerrit.Hendriksen@deltares.nl
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
Script is used to import chloride measuruments and for take up in datamodel for timeseries
"""
import os
import pandas as pd
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def establishconnection(fc, connectionsstring=None):
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
    elif fc == None and connectionsstring != None:
        engine = create_engine(connectionsstring, echo=False)

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
    fc = r"C:\develop\nwdm_upserts\config_local.txt"
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
session, engine = establishconnection(None, connstr)

# tables
# below table connections could be administrated in an excel sheet whit followin structure
# organisation, path_datatable, path_mappingtable, fileformat, crs
#
# where organisation is similar to the table name that will be imported
mappingnioz = r"C:\projectinfo\nl\NWDM\mappingRWSNIOZacidity.csv"
datanioz = (
    r"C:\develop\nwdm\nioz_data\RWS-NIOZ North Sea data v2023_10 for SDG14-3-1.xlsx"
)
datasocat = r""
mappingsocat = r""

# delete the current import table
dcttbls = {}
dcttbls["nioz"] = (datanioz, mappingnioz, "xls")
dcttbls["socat"] = (datasocat, mappingsocat, "csv")
dcttbls["tablename"] = (datafile, mappingfile, "format")


# loop over all the records in the table setup above

schema = "import"
for tbl in dcttbls.keys():
    # clear up old data
    strsql = f"drop table if exists {schema}.mapping_{tbl}"
    engine.execute(strsql)
    strsql = f"drop table if exists {schema}.{tbl}"
    engine.execute(strsql)
    # based on the format use differen read function of PANDAS
    data = dcttbls[tbl][0]
    if dcttbls[tbl][2] == "csv":
        dfdata = pd.read_csv(data)
    elif dcttbls[tbl][2] == "xls":
        dfdata = pd.read_excel(data)
    dfmapping = pd.read_csv(dcttbls[tbl][1])
    dfdata.to_sql(tbl, engine, schema=schema)
    dfmapping.to_sql("mapping_" + tbl, engine, schema=schema)

    # should be extended with adding the reference to datatable name to one of the tables
    # should be extended with an statement that creates the geometry

### create backup of existing tables
"""select m.* into nwdm._backup_measurement_nioz from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ';"""
"""select d.* into nwdm._backup_dataset_nioz from nwdm.dataset d where d.data_owner ='NIOZ';"""
"""select l.* into nwdm._backup_location_nioz from nwdm.location l  where l.data_owner ='NIOZ';"""
