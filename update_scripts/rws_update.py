############################################################	
# Imports
from sqlalchemy.sql import text
import ddlpy
import datetime as dt
import pandas as pd
import geopandas as gpd
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
import utils


#############################################################
# Connect to the database

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

############################################################
# Collect from database the locations from sione and rawdata table 
# TODO: in the future it is going to collect only from one table 
 

query = '''select distinct loc as location from nwdm.import.donar_sioene
union
select distinct locatie_code as location from nwdm.import.rawdata'''
response = engine.connect().execute(text(query)).fetchall()
nwdm_locations = [r[0] for r in response]


#############################################################
# Collect from the database the parameters that where already there
query = '''SELECT mp.sdn_p01_code, mp.aquo_grootheid_code, mp.aquo_chemischestof_code, mp.aquo_hoedanigheid_code, mp.aquo_compartiment_code 
FROM import.mapping_sdn_p01 AS mp
WHERE mp.sdn_p01_code IN (SELECT DISTINCT parameter_code 
                          FROM nwdm.measurement_p01_all 
                          WHERE data_owner = 'Rijkswaterstaat');'''
#execute query and keep the column names
response = engine.connect().execute(text(query))
data = response.fetchall()
columns = response.keys()

# create a dataframe with the columns and the parameters
parameters_df = pd.DataFrame(data, columns=columns)
#filter out from the parameters_df the rows where the values of the column 'aquo_chemischestof_code' are NVT






############################################################
# Get the locations from DDLPY and subset them based on the needs of NWDM.
all_locations_df = ddlpy.locations()
#add as column the code
all_locations_df["Code"] = all_locations_df.index
nwdm_locations_df = all_locations_df[all_locations_df.index.isin(nwdm_locations)]

#filter nwdm_locations_df to only keep the rows where the values of the column 'Parameter.code' are in the parameterd_df["aquo_chemischestof_code"]
filtered_nwdm_location_df = nwdm_locations_df[
    (nwdm_locations_df['Parameter.Code'].isin(parameters_df['aquo_chemischestof_code'])) &
    (nwdm_locations_df['Grootheid.Code'].isin(parameters_df['aquo_grootheid_code'])) &
    (nwdm_locations_df['Hoedanigheid.Code'].isin(parameters_df['aquo_hoedanigheid_code'])) &
    (nwdm_locations_df['Compartiment.Code'].isin(parameters_df['aquo_compartiment_code']))
]



############################################################	
# Get the measurements from the locations
#1. Set start date and end date different

start_date = dt.datetime(1920, 1, 1)
# end date is now
end_date = dt.datetime.now()
measurements = []

for i in range(0,len(filtered_nwdm_location_df)):
    location_measurements_df = ddlpy.measurements(filtered_nwdm_location_df.iloc[i], start_date=start_date, end_date=end_date)
    if location_measurements_df.empty:
        print(f"No data for location {filtered_nwdm_location_df.iloc[i]['Code']}") 
    else:
        print(f"Data for location {filtered_nwdm_location_df.iloc[i]['Code']}")
        measurements.append(location_measurements_df)
if len(measurements) > 0:       
    measurements_df = pd.concat(measurements)
        
   

############################################################
# TODO: check if the datetime is in the appropriate format
# TODO: rename the columns all to lower case and without dots
# TODO: check how the location was registered in the old database. 
# TODO: check what needs to be altered to the location table

############################################################
# gdf = gpd.GeoDataFrame(renamed_measurements, geometry=gpd.points_from_xy(measurements['X'], measurements['Y']))
#         # set the crs to epsg=25831
# gdf.crs = "EPSG:25831"
#         # transform the dataframe from 25831 to 28992
# gdf = gdf.to_crs(epsg=28992)
        





