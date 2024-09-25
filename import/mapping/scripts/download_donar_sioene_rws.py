import sqlalchemy as sa
from sqlalchemy.sql import text
import ddlpy
import datetime as dt
import pandas as pd
import geopandas as gpd

# connect to the database
engine = sa.create_engine("postgresql+psycopg2://micha:cB3Ab8DvXAdim2ExpMpmvGRQowpZtJ@c-oet17813.directory.intra/nwdm")
connection = engine.connect()
# collect all the sensors names
query = 'select distinct loc from nwdm.import.donar_sioene;'
result = connection.execute(text(query)).fetchall()
result = [r[0] for r in result][:1]
#result = ["NOORDWK20", "ALMLO"]
start_dates = []
for r in result:
    query = f"select max(ds.datumtijdwaarde) from import.donar_sioene ds where ds.loc = '{r}';"
    result_date = connection.execute(text(query)).fetchall()
    if result_date[0][0] is not None:
        result_date = dt.datetime.strptime(result_date[0][0], '%d-%m-%Y %H:%M:%S')
        start_dates.append(result_date)

# Dictionary to rename columns
rename_dict = {
    'WaarnemingMetadata.BemonsteringshoogteLijst': 'plt_bmh',
    'Meetwaarde.Waarde_Numeriek': 'waarde',
    'Code': 'loc',
    'WaarnemingMetadata.KwaliteitswaardecodeLijst': 'kwc',
    'WaarnemingMetadata.ReferentievlakLijst': 'plt_refvlak',
    'Eenheid.Code': 'ehd',
    'Parameter.Code': 'par',
    'Hoedanigheid.Code': 'hdh'
}

# get data
# get the dataframe with locations and their available parameters
locations = ddlpy.locations()
sub_locations = locations[locations.index.isin(result)]
# search for the word silica in the column Parameter_Wat_Omschrijving
#bool_silica = sub_locations['Parameter_Wat_Omschrijving'].str.contains('siliciumdioxide', case=False)
#select a set of parameters
# Filter the locations dataframe with the desired parameters and stations.
# timeseries (NVT) versus extremes
bool_groepering = sub_locations[sub_locations['Parameter.Code'].isin(['SiO2'])]


# end date is the current date
end_date = dt.datetime.now()
# provide a single row of the locations dataframe to ddlpy.measurements

data = []
no_data = []
all_data = []
for i in range(0, len(bool_groepering)):
    measurements = ddlpy.measurements(bool_groepering.iloc[i], start_date=start_dates[i], end_date=end_date)
    if measurements.empty:
        no_data.append(result[i])
    else:
        data.append(result[i])
        renamed_measurements = measurements.copy()
        # Rename columns
        renamed_measurements.rename(columns=rename_dict, inplace=True)
        # Keep only the specified columns
        renamed_measurements = renamed_measurements[list(rename_dict.values())]
        renamed_measurements['datum'] = measurements.index.strftime('%Y%m%d')
        renamed_measurements['tijd'] = measurements.index.strftime("'%H%M")
        # to geopandas dataframe
        gdf = gpd.GeoDataFrame(renamed_measurements, geometry=gpd.points_from_xy(measurements['X'], measurements['Y']))
        # set the crs to epsg=25831
        gdf.crs = "EPSG:25831"
        # transform the dataframe from 25831 to 28992
        gdf = gdf.to_crs(epsg=28992)
        all_data.append(gdf)
print(no_data)
print(data)
# make a dataframe with the data
df = pd.concat(all_data)
df['cpm'] = 10
print(df)
# save the dataframe to a csv file
df.to_csv(r"silica_data.csv", index=False)



