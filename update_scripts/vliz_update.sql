select count(*) from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='VLIZ'
;

-- create backup
select m.* into nwdm._backup_measurement_vliz from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='VLIZ';
select d.* into nwdm._backup_dataset_vliz from nwdm.dataset d where d.data_owner ='VLIZ';
select l.* into nwdm._backup_location_vliz from nwdm.location l  where l.data_owner ='VLIZ';


-- delete VLIZ data
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner ='VLIZ');
delete from nwdm.dataset where data_owner ='VLIZ';
delete from nwdm.location where data_owner ='VLIZ';

-- back up the old import table

-- run again the create_objects_import.sql for the vliz (some of the columns changed order)

drop table if exists import.vliz_station_abiotic;
CREATE TABLE import.vliz_station_abiotic
(
  Station TEXT
, Latitude TEXT
, Longitude TEXT
, "Time" TEXT
, "Ammonium_NH4(umol_N_NH4/L)" TEXT
, "Conductivity(uS/cm)" TEXT
, CPAR TEXT
, "Density(kg/m3)" TEXT
, "Fluorescence(mg/m3)" TEXT
, "Nitrate_Nitrite(umol_N_NO3_NO2/L)" TEXT
, "Nitrate_NO3(umol_N_NO3/L)" TEXT
, "Nitrite_NO2(umol_N_NO2/L)" TEXT
, "OBS(NTU)" TEXT
, PAR TEXT
, Ph TEXT
, "Phosphate_PO4(umol_P_PO4/L)" TEXT
, "Pressure(db)" TEXT
, "Salinity(PSU)" TEXT
, "Secchi_Depth(cm)" TEXT
, "Silicate_SiO4(umol_Si_SiO4/L)" TEXT
, "Sound_Velocity(m/s)" TEXT
, SPAR TEXT
, "SPM(mg/l)" TEXT
, "Temperature(degC)" TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

select * from import.vliz_station_abiotic;