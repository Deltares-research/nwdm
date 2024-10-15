-- a few comments on the update process

select count(*) from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ'
;

-- back up the old import table

-- create backup
select m.* into nwdm._backup_measurement_nioz from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ';
select d.* into nwdm._backup_dataset_nioz from nwdm.dataset d where d.data_owner ='NIOZ';
select l.* into nwdm._backup_location_nioz from nwdm.location l  where l.data_owner ='NIOZ';

-- delete old data
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner ='NIOZ');
delete from nwdm.dataset where data_owner ='NIOZ';
delete from nwdm.location where data_owner ='NIOZ';

-- run the python script to pre-process and import the data

--RUN AGAIN THE verwerk_nioz.sql /located in the etl folder (part of the pentaho workflow) 
--(the verwerk_nioz.sql had to be adjusted to work withe the new data) see below the new version of the script

--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('NIOZ',3) on conflict do nothing ;


-- 5. nioz data
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id as location_id
, rd.datetime::date as "date"
, case when rd.depth_nominal='-999' then null else depth_nominal end ::numeric as depth
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, case when rd.cvalue = '-999' then null else replace(rd.cvalue, '<','') end ::decimal  as value
, qua.quality_id as quality_id
from (select
    cnr, cname, cvalue
    , case cname    when 'dic' then dic_flag
                    when 'alkalinity' then alkalinity_flag
                    when 'phspectro_total_lab' then ph_spectro_flag 
                    -- for the parameters that they don't have a flag, we added a flag 0 which means no quality control 
                    -- we did that in order to keep temperature and salinity but exclude on the same time the bad flag values from the parameters that they have flag
                    when 'temperature' then '0'
                    when 'salinity' then '0'
                    when 'ph_spectro_temperature' then '0'
                    when 'nitrate' then '0'
                    when 'nitrite' then '0'
                    when 'ammonia' then '0'
                    when 'phosphate' then '0'
                    when 'silicate' then '0'
                    when 'doc' then '0' end as flag
    ,station, bottle_id, datetime, depth_nominal, _recordnr, _short_filename, _path
from (select unnest(array [1,2,3,4,5,6,7,8,9,10,11,12]) as cnr
              , unnest(array ['temperature','salinity','dic','alkalinity','phspectro_total_lab', 'ph_spectro_temperature', 'nitrate', 'nitrite', 'ammonia', 'phosphate', 'silicate', 'doc']) as cname
              , replace(unnest(array ["temperature","salinity","dic","alkalinity","ph_spectro_total_lab", "ph_spectro_temperature", "nitrate", "nitrite", "ammonia", "phosphate", "silicate", "doc"]), ',',
                        '.') as cvalue 
              , dic_flag
              , alkalinity_flag
              , ph_spectro_flag
              ,station, bottle_id, datetime, depth_nominal, _recordnr, _short_filename, _path
         from (select * from import.nioz) sub) sub2
         where sub2.cvalue is not null and sub2.cvalue <> '-999'
) rd
left join import.mapping_nioz m on m.originalname = rd.cname
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on lower(loc.location_code) = 'nioz_'||lower(rd.station) and loc.data_owner='NIOZ'
join nwdm.quality qua on qua.code = rd.flag and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = m.p06
join nwdm.parameter par01 on par01.code = m.p01
;
      