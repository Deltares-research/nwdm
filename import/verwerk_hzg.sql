--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('HZG',4) on conflict do nothing ;

-- 4. ferrybox data Helmholtz Zentrum Geesthacht - COSYNA
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 300000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'HZG' data_holder
, 'HZG' data_owner
, 'https://repos.deltares.nl/repos/Wozep/trunk/NWDM/ferrybox_data' link_to_data
, 'http://codm.hzg.de/codm' link_to_metadata
from import.ferrybox_header fh
group by "_short_filename", "_path";

drop table if exists import._temp_ferryboxdata_geom;
select _recordnr
, st_setsrid(st_makepoint(sub.x,sub.y),4326) as geom
, null::bool in_scope_north_sea
into import._temp_ferryboxdata_geom
from (
         select _recordnr
              , case when lon != 'None' then lon end::numeric as x
              , case when lat != 'None' then lat end::numeric as y
         from import.ferrybox_data
     ) sub
;

-- bepaal geom in scope
update import._temp_ferryboxdata_geom fdg set in_scope_north_sea = st_contains((select geom from nwdm.scope_northsea), fdg.geom);


-- 4. observations ferrybox HZG
set DateStyle to 'DMY';
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id, geom)
select
fd._recordnr as recordnr_dataset
, null as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, null as location_id
, fd.time::timestamp as "date"
, case when fd.depth='None' then null else depth end ::numeric as depth
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, replace(replace(fd.value,',','.'), '<','')::decimal  as value
, qua.quality_id as quality_id
, fdg.geom
from import.ferrybox_data fd
join import._temp_ferryboxdata_geom fdg on fdg._recordnr=fd._recordnr
join import.ferrybox_header fh on fh._short_filename=fd._short_filename
left join import.mapping_ferrybox_parameter fp on lower(fp.parameter) = lower(fd.parameter)
left join import.mapping_ferrybox_unit fu on lower(fu.headerline )= lower(substring(fh.headerline from '%#"value_%_#",flag%' for '#'))
join nwdm.dataset ds on ds."path"=fh."_path" and ds.short_filename=fh."_short_filename"
join nwdm.quality qua on qua.code = fd.flag and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
join nwdm.unit un on un.code = fu.p06
join nwdm.parameter par01 on par01.code = fp.p01
where fdg.in_scope_north_sea=true;
;
