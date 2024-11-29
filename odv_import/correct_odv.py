from os import name, replace
from numpy import NaN, inner
import numpy as np
import pandas as pd
from pandas.core.dtypes.missing import notna
from pandas.core.indexes.base import Index
import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.sql.base import ColumnSet
from sqlalchemy.sql.expression import column, true
from itertools import islice

engine = create_engine('postgresql://pg:pg@localhost:5432/emodnet')
db_schema = 'odv'
conn = engine.connect()

# variables
begin_time = datetime.datetime.now()
# odv_file = r'd:\nwdm_data\emodnet\North_Sea_eutrophication_and_acidity_aggregated_v2018_2.txt'
odv_file = r'd:\nwdm_data\emodnet\test.odv'
odv_file_corrected = odv_file + '_corrected.odv'
list_vars_in_sample = False
startline_data = 0
# REPLACE DUBBELE TABs: GEOTERM81-83		  - Unknown (ZZ99)
str_2correct = 'GEOTERM81-83		  - Unknown (ZZ99)'
str_corrected = 'GEOTERM81-83 - Unknown (ZZ99)'

header_metavar = []
header_datavar = []

def show_time(info):
    print(info + ': '+ str(datetime.datetime.now() - begin_time))

# analyseer header 
with open(odv_file) as f:
    for index, line in enumerate(f):
        if(line.startswith('//')):
            if(line.startswith('//<MetaVariable>')):
                header_metavar.append(line)
            if(line.startswith('//<DataVariable>')):
                header_datavar.append(line)
            startline_data = index
        else:
            break
f.close()

# df_datavar = pd.DataFrame(header_datavar)
# df_datavar.to_sql('datavar', engine, schema=db_schema, if_exists='replace', index=False)

# print(df_datavar)

startline_data += 1
nr_data_columns = len(header_metavar) + 1 + 2*len(header_datavar) + 1

show_time('analyse header')

# analyseer aantal kolommen & rapporteer evt mismatch
print('columns expected: '+ str(nr_data_columns))
df_columns_per_line = pd.DataFrame(columns=['linenr','columns'])
fr = open(odv_file, 'r')
fw = open(odv_file_corrected, 'w')
# with open(odv_file) as f:
for linenr,line in islice(enumerate(fr), 0, None):
    columns_in_line = line.count('\t') + 1
    if(linenr >= startline_data and columns_in_line != nr_data_columns):
        df_columns_per_line = df_columns_per_line.append({'linenr':linenr+1,'columns':columns_in_line}, ignore_index=True)
        line = line.replace(str_2correct, str_corrected)
    fw.write(line)
fr.close()
fw.close()

# print(df_columns_per_line)
# df_columns_per_line.to_sql('columns_per_line', engine, schema=db_schema, if_exists='replace', index=False)
print(df_columns_per_line)

show_time('analyse columns')

