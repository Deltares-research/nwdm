--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('SOCAT',7) on conflict do nothing ;


--6. socat data
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 500000 dataset_id
, 'SOCATv2020' as dataset_name
,'data_from_SOCATv2020.txt' short_filename
, '' "path"
, 'data_from_SOCATv2020.txt' as file
, count(*) number_of_records
, 'SOCAT' data_holder
, 'SOCAT' data_owner
, 'https://repos.deltares.nl/repos/Wozep/trunk/NWDM/socat_data' link_to_data
, 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/metadata/0210600.html' link_to_metadata
;

-- geom bepalen & scope northsea toepassen
drop table if exists import._temp_socat_geom;
select _recordnr
    ,st_setsrid(st_makepoint(lon::decimal, lat::decimal),4326) as geom
    ,null::bool in_scope_north_sea
into import._temp_socat_geom
from import.socat;

update import._temp_socat_geom s set in_scope_north_sea = st_contains((select geom from nwdm.scope_northsea), s.geom);

-- observations SOCAT
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id,geom)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, (select dataset_id from nwdm.dataset where data_owner='SOCAT') as dataset_id
, null::int as location_id
, rd.datetime::timestamp as "date"
, rd.sample_depth_m ::numeric as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, case when rd.cvalue = '-999' or rd.cvalue='-9999' then null else rd.cvalue::decimal end as value
, qua.quality_id as quality_id
, rd.geom
-- select count(*)
from (
    select
    cnr, cname, cvalue
    , case cname    when 'water_temperature_degc' then qf_water_temperature
                    when 'salinity_psu' then qf_salinity
                     end as flag
    ,station, datetime, sample_depth_m, qf_sample_depth, _recordnr
    , geom
    from (
         select unnest(array [1,2,3])                                                           as cnr
         , unnest(array ['water_temperature_degc','salinity_psu','fco2_recomputed_uatm']) as cname
         , replace(unnest(array ["water_temperature_degc","salinity_psu","fco2_recomputed_uatm"]), ',', '.') as cvalue -- komma=duizendscheidingsteken
         , qf_water_temperature
         , qf_salinity
         ,station, datetime, sample_depth_m, qf_sample_depth, s._recordnr
         ,geom
         from import.socat s join import._temp_socat_geom g on g._recordnr=s._recordnr
        where g.in_scope_north_sea=true
   ) sub
    where cvalue is not null
) rd
left join import.mapping_socat m on m.column_name = rd.cname
join nwdm.quality qua on qua.code = coalesce(rd.flag,'0') and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join import.mapping_matrix mmx on mmx.compartiment_code = 'OW'		--rd.compartiment_code
left join nwdm.unit un on un.code = m.p06
join nwdm.parameter par01 on par01.code = m.p01 --5850600
;