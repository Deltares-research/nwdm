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

import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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


def read_config(file):
    """ Read the configuration file and return the configuration object """
    cf = configparser.ConfigParser()
    cf.read(file)
    return cf