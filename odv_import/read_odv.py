from os import name, replace
# from numpy import NaN, inner
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

# variables
begin_time = datetime.datetime.now()
# odv_file = r'D:\data\nwdm\EMD_Eutrophication_NorthSea_2022_unrestricted\Eutrophication_NorthSea_non-nutrient_profiles_2022_unrestricted.txt'
odv_file = r'D:\data\nwdm\EMD_Eutrophication_NorthSea_2022_unrestricted\Eutrophication_NorthSea_non-nutrient_timeseries_2022_unrestricted.txt'
# odv_file = r'D:\data\nwdm\EMD_Eutrophication_NorthSea_2022_unrestricted\Eutrophication_NorthSea_nutrient_profiles_2022_unrestricted.txt'
# odv_file = r'D:\data\nwdm\EMD_Eutrophication_NorthSea_2022_unrestricted\Eutrophication_NorthSea_nutrient_timeseries_2022_unrestricted.txt'
# cname1_subset = 'Depth [m]'
cname1_subset = 'time_ISO8601'
# cname1_subset = ""
cname_p01 = 'P01 Codes'
main_keycols = ["LOCAL_CDI_ID", "EDMO_code"]
mainid = '_mainid'
subid = '_subid'
sub_keycols = [mainid, subid]
list_vars_in_sample = False
startline_data = 0
# str2correct = 'Chemical 	oceanography'

# db connection
engine = create_engine('postgresql://postgres:pg@localhost:5432/emodnet')
# engine = create_engine('postgresql://USER:WW@c-oet17813.directory.intra:5432/nwdm')
# db_schema = 'odv'
db_schema = 'import'
conn = engine.connect()

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
                # if cname1_subset=="":
                    # cname1_subset = line[line.find('"') + 1 : line.find('"', line.find('"') + 1)] if '"' in line else None
            startline_data = index
        else:
            break
f.close()

print('cname1_subset: ', cname1_subset)

startline_data += 1
nr_data_columns = len(header_metavar) + 1 + 2*len(header_datavar) + 1

show_time('analyse header')

# analyseer aantal kolommen & rapporteer evt mismatch
print('columns expected: '+ str(nr_data_columns))
df_columns_per_line = pd.DataFrame(columns=['linenr','columns'])
# f = open(odv_file, 'r')
# f = open(odv_file, 'w')
with open(odv_file) as f:
    for linenr,line in islice(enumerate(f), startline_data, None):
        columns_in_line = line.count('\t') + 1
        # print(columns_in_line)
        if(columns_in_line != nr_data_columns):
            df_columns_per_line = df_columns_per_line.append({'linenr':linenr+1,'columns':columns_in_line}, ignore_index=True)
f.close()
df_columns_per_line.to_sql('columns_per_line', engine, schema=db_schema, if_exists='replace', index=False, method='multi')
print(df_columns_per_line)

show_time('analyse columns')

# LEES ODV-BESTAND IN IN DATAFRAMES
# read file and put it in dataframe
df = pd.read_csv(odv_file,delimiter="\t", engine='python', skiprows=range(0,startline_data), header=0,)

# print columns subset
print(df.columns)

# bepaal grens hoofd- en subset
cnr1_subset = df.columns.get_loc(cname1_subset)

print(cnr1_subset)

# bepaal columns hoofdset
maincols = df.iloc[:,:cnr1_subset].columns

# fill empty columns with values above (NB alleen voor columns uit hoofdset; dit zou zelfs voor enkel de sleutelkolommen kunnen!?)
df.loc[:,maincols] = df.loc[:,maincols].ffill()

# voeg mainid toe voor elk hoofdrecord (op grens van hoofd- en subset)
df.insert(cnr1_subset,mainid,df.groupby(main_keycols, sort=False).ngroup())

# splits df op in 2 delen (hoofd en sub)
df_main = df.iloc[:,:cnr1_subset+1]
df_sub = df.iloc[:,cnr1_subset:]

# voeg subid toe
df_sub.insert(loc=1,column=subid,value=df_sub.groupby(mainid, sort=False).cumcount()+1)

# comprimeer df_main (ofwel: bewaar enkel het eerste record)
df_main = df_main.groupby(mainid,as_index=False).first()

show_time('inlezen sample')

# LOOP DOOR SUBDATA TBV OBSERVATIONS (VARIABLE EN WAARDE)
df_obs = pd.DataFrame()
sc_qualname = ''
df_vars = pd.DataFrame(columns=['variable'])
for sc_name in df_sub.columns:

    # skip: id-kolommen, laatste kolom, elke quality-kolom (wordt binnen loop erbij gejoint)
    if(sc_name not in sub_keycols and sc_name != 'QV:SEADATANET:SAMPLE' and sc_name != sc_qualname):

        # determine next column = quality flag-column
        cval_nr = df_sub.columns.get_loc(sc_name)
        cqual_nr = cval_nr + 1
        sc_qualname = df_sub.columns[cqual_nr]

        # fill df_vars
        di_vars = {'variable': sc_name}
        # df_vars = df_vars.append(di_vars,ignore_index=True)
        df_vars = pd.concat([df_vars, pd.DataFrame([di_vars])], ignore_index=True)

        # melt variable value + quality indicator
        mc = sub_keycols + [sc_name, sc_qualname]
        dfv = pd.melt(df_sub[mc],id_vars=sub_keycols, value_vars=[sc_name])
        dfq = pd.melt(df_sub[mc],id_vars=sub_keycols, value_vars=[sc_qualname])
        
        # drop empty observations
        dfv.dropna(subset=['value'],inplace=True)

        # join variable and quality values and add to df_obs (all observations)
        dfv = dfv.set_index(sub_keycols)
        dfqv = dfv.join(dfq.set_index(sub_keycols), on=sub_keycols, rsuffix='_q').drop(columns=['variable_q']).rename(columns={'value_q':'quality'})
        df_obs = df_obs.append(dfqv)
        # df_obs = pd.concat([df_obs, pd.DataFrame([dfqv])])

# convert variable to _varid
df_vars.index.name='_varid'
df_vars.reset_index(inplace=True)
df_vars['_varid'] += 1
df_obs.reset_index(inplace=True)
df_obs = pd.merge(left=df_obs, right=df_vars, how='inner', on='variable', sort=False).drop(columns=['variable'])
# sorteer kolommen
df_obs = df_obs.reindex(columns=['_mainid','_subid','_varid','value','quality'])

show_time('inlezen observation')

if(list_vars_in_sample):
    # GET VARIABLES FROM OBSERVATIONS
    # quality-kolommen weglaten
    df_sub1 = df_sub[sub_keycols + df_vars['variable'].values.tolist()]

    # nieuwe kolom tbv variable-id's toevoegen
    df_main['_varid'] = None

    # loop door rijen (van eerste sub-record) en relateer deze aan de p01-codes uit het hoofdrecord
    for key, row in df_sub1.iterrows():
        # lijst van kolommen (=variabelen) met gevulde waarde 
        s_cols = row.notna()
        s_cols = s_cols[s_cols == True]
        s_cols = s_cols.drop(sub_keycols)
        s_cols = s_cols.index.tolist()

        # variabelen omzetten naar _varid
        df_x = pd.DataFrame({'variable':s_cols})
        df_x = df_x.merge(df_vars,on='variable',how='inner')
        li_vars = df_x['_varid'].values.tolist()
        li_vars_str = [str(element) for element in li_vars]
        # list met _varid omzetten naar string
        str_vars = ",".join(li_vars_str)
        # string met _varid's in df_main wegschrijven
        df_main.loc[df_main._mainid==row[mainid],'_varid']  = str_vars

    show_time('variabelen met waarden bepalen en in sample plaatsen')

# write to db
# df.to_sql('df', engine, schema=db_schema, if_exists='replace', index=True)
df_main.to_sql('odv_sample2', engine, schema=db_schema, if_exists='replace', index=False)
# df_sub.to_sql('df_sub', engine, schema=db_schema, if_exists='replace', index=False)
df_obs.to_sql('odv_observation2', engine, schema=db_schema, if_exists='replace', index=False)
df_vars.to_sql('odv_variable2', engine, schema=db_schema, if_exists='replace', index=False)

show_time('wegschrijven in db')

# TO DO
# wat te doen met laatste (extra) QV-kolom?  ("QV:SEADATANET:SAMPLE"; is dat soms een overall-flag voor het hoofdrecord?)
# skiprows variabel maken
# columnnr 'cname1_subset' variabel maken (obv metadata? of is dit altijd Depth?)
