"""
create csv files as mapping tables 
"""

import os
import csv
import urllib.request
import vocabrel

# from sqlalchemy import create_engine
# import sqlfunctions

def makecsv(mapping, filename):
    kv = []
    for k in mapping:
        for v in mapping[k]:
            kv.append([k, v])
    with open(filename, 'w', newline='') as f:
        wr = csv.writer(f)
        for kk in kv:
            wr.writerow(kk)
    return

# make mappings
mapping_p35_p01 = vocabrel.sdn_mapping('P35','P01',level='narrower')
mapping_p06_p01 = vocabrel.sdn_mapping('P06','P01',level='related')

# create CSVs
makecsv(mapping_p35_p01, '..//mapping_p35_p01.csv')
makecsv(mapping_p06_p01, '..//mapping_p06_p01.csv')

    

# create P01, P06 csv


## Add vocabs using the function vocab2orm
# def vocab2orm(xmlfile,obj,type=0):
# #    print('initialize: inserting: ' + xmlfile + ', please wait ...')
#     import bodc
#     if type==0:
#        vocab = bodc.fromfile_collection(xmlfile)
#     elif type==1:
#        vocab = bodc.fromfile_list(xmlfile)
#     for i in range(len(vocab["identifier"])):
#         exist1 = session.query(obj).filter_by(identifier=vocab["identifier"][i]).first()
#         if exist1==None:
#             print ' '.join(['adding:',vocab["identifier"][i],vocab["prefLabel"][i]])
#             if xmlfile.split('.')[0] == 'P01' or xmlfile.split('.')[0] == 'P35':
#                 row = obj (identifier  = vocab["identifier"][i], 
#                            preflabel   = vocab["prefLabel"][i],
#                            altlabel    = vocab["altLabel"][i], 
#                            definition  = vocab["definition"][i],
#                            origin      = xmlfile.split('.')[0])
#             else:
#                 row = obj (identifier  = vocab["identifier"][i], 
#                            preflabel   = vocab["prefLabel"][i],
#                            altlabel    = vocab["altLabel"][i], 
#                            definition  = vocab["definition"][i])
                    
#             session.add(row)
#             session.commit()
#         else: # so the parameter exists and will be updated with proper labels
# #            print i
#             a = vocab["altLabel"][i]
#             p = vocab["prefLabel"][i]
# #            p = p.replace("""'""",'`')
# #            p = p.replace("""+""",' plus ')
#             print(i,p)
#             p = p.encode('utf8')
#             p = p.strip()
#             _id = vocab["identifier"][i]
#             strSql = "UPDATE parameter SET altlabel = $${a}$$, preflabel = $${p}$$ WHERE identifier = '{id}' ".format(a=a,p=p,id=_id)
#             try:
#                 engine.execute(strSql)
#                 #sqlfunctions.perform_sql(strSql,credentials)
#             except:
#                 print ' '.join([vocab["altLabel"][i],(vocab["prefLabel"][i]).replace("""'""",'`').replace("""+""",' plus ').encode('utf-8').strip(),vocab["identifier"][i]])
#                 print strSql
            

# ## Get files cache of vocabs
# tmpdir = tempfile.gettempdir()
# items =['P35','P06','P01','L20','P36','L04','P02','S27']
# for item in items:
#     if not(os.path.isfile(os.path.join(tmpdir,item + '.xml'))):
# #        print 'retrieving', tmpdir,item + '.xml'
#         urllib.urlretrieve("http://vocab.nerc.ac.uk/collection/"+item+"/current/", os.path.join(tmpdir,item+'.xml'))
#     else:
#         print ' '.join(['file', tmpdir,item + '.xml', 'already there'])

# # map the P35 to the P01
# mapping = vocabrel.sdn_mapping('P35','P01')
# i=0
# mapping = vocabrel.sdn_mapping('P35','P01')
# for k in mapping:   # for each key
#     for v in mapping[k]:
#         strSql = """select id from p35 where identifier = '{v}'""".format(v=v)
#         a = sqlfunctions.executesqlfetch(strSql,credentials)
#         if len(a)>0:
#             strSql = """UPDATE parameter SET p35_id = '{p35id}' 
#                         where identifier='{p01id}'""".format(p35id=a[0][0],p01id=k)
#             sqlfunctions.perform_sql(strSql,credentials) 


# ## Add z types, after p01
# codes = ['PRESPR01','ADEPZZ01','COREDIST','MINWDIST','MAXCDIST','MINCDIST','MAXDIST']
# units = ['ULAA'    ,'UPDB'    ,'ULAA'    ,'ULAA'    ,'ULAA'    ,'ULAA'    ,'ULAA'   ]

