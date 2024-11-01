When the nwdm database has been created the rws data has been downloaded from the thredds server
or have been provide by rws with csv (the latter one was not added correctly in the database in the past).

Now the data are available via the ddlpy package of deltares
https://deltares.github.io/ddlpy
In the shortfilename and path, datasource and metadata I will put this link.

The data will not be kept locally but directly in the database.
Steps on how to download them and process them before we write them in the database are in the rws_update.py.

In the rws_update.sql the measurements/location/dataset table are updated.

I kept the thredds dataset and I added also the data from the ddlpy. 
Number of records in thredds: 1426661; max date: 2018-12-31T22:00:00Z
Number of records in ddlpy: 253,313; 2023-12-14 14:12:00.000


To distinghuish the locations as they have the same names, in the ddlpy case I added ddlpy in front.
