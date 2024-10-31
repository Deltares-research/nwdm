"""
Load and pre-process the data from nioz
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

#paths to mapping and data files
mapping_table_file = r"C:\develop\nwdm\etl\mapping\mappingRWSNIOZacidity.csv"

data_table_file = (
    r"C:\develop\nwdm\data\RWS-NIOZ North Sea data v2023_10 for SDG14-3-1.xlsx"
)


mapping_table= "mapping_nioz"
data_table = "nioz"

schema = "import"

# delete the current import tables
# mapping table
""" strsql = f"drop table if exists {schema}.{mapping_table}"
with engine.connect() as conn:
    result = conn.execute(text(strsql))
#data table
strsql = f"drop table if exists {schema}.{data_table}"
with engine.connect() as conn:
    result = conn.execute(text(strsql)) """

####################################################33
# Pre-processing the data (mapping table)
df_mapping = pd.read_csv(mapping_table_file, sep=';')
df_mapping["_recordnr"] = df_mapping.index + 1
df_mapping["_short_filename"] = "mappingRWSNIOZacidity.csv"
df_mapping["_path"] = "https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg"
df_mapping.columns = df_mapping.columns.str.lower()
# make all values in the column originalname to lower case
df_mapping['originalname'] = df_mapping['originalname'].str.lower()

df_mapping.to_sql(mapping_table, engine, schema=schema, index=False)


# Pre-processing of the data (data table)
df_data = pd.read_excel(data_table_file)

#Make sure that the columns are in lower case
df_data.columns = df_data.columns.str.lower()

#Add _recordnr, _short_filename, _path
df_data["_recordnr"] = df_data.index + 1
df_data["_short_filename"] = "mappingRWSNIOZacidity.csv"
df_data["_path"] = "https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg"


#merge date_utc (datetime64) and time_utc(str) column in one datetime column
df_data['date_utc'] = df_data['date_utc'].astype(str)
df_data['time_utc'] = df_data['time_utc'].astype(str)
df_data['datetime'] = df_data['date_utc'] + ' ' + df_data['time_utc']
df_data.drop(columns=['date_utc', 'time_utc'], inplace=True)
df_data['datetime'] = pd.to_datetime(df_data['datetime'], errors='coerce')

#transform all columns to text type to avoid errors when writing to the database except the _recordnr column
for col in df_data.columns:
    if col != '_recordnr':
        df_data[col] = df_data[col].astype(str)    


#write them to the database but without a column for the index
df_data.to_sql(data_table, engine, schema=schema, index=False)

   

