--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('PML',9) on conflict do nothing ;


--9. PML dataset
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner='PML');
delete from nwdm.dataset where data_owner='PML';
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 500500 + _recordnr dataset_id
, replace(bestand, '.xlsx', '') as dataset_name
,"_short_filename" short_filename
, "pad" "path"
, pad ||'\'||"bestand" as file
, null number_of_records
, 'PML' data_holder
, 'PML' data_owner
, 'P:\11205327-003-jerico-s3\data\PML_primprod\' link_to_data
, 'https://www.bodc.ac.uk/' link_to_metadata
from import.pml_primprod_info;


--9. PML locations
delete from nwdm.location where data_owner='PML';
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
    'pml_'|| rd._recordnr as location_code
    , coalesce(rd.station, 'station'||rd._recordnr::varchar) as location_name
    , (rd.x)::decimal as x
    , (rd.y)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(x,y), 4326::int) as geom
    , 'PML' data_owner
    from (
        select *
        , (case when longitude_dec_degrees > 40.0 and latitude_dec_degrees < 0.0 then latitude_dec_degrees else longitude_dec_degrees end)::decimal x
        , (case when longitude_dec_degrees > 40.0 and latitude_dec_degrees < 0.0 then longitude_dec_degrees else latitude_dec_degrees end)::decimal y
                from import.pml_primprod
    ) rd
    -- where not(rd.x::decimal between 9.0 and 43.0 and rd.y::decimal between 9.0 and 43.0)        -- weglaten locaties rondom Corsica
) g where st_contains((select geom from nwdm.scope_northsea),g.geom)
;



-- PML data
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, null::int as recordvolgnr_dataset
, ds.dataset_id
, loc.location_id
, rd.date::date as "date"
-- , null as begin_time
-- , null as end_time
, null as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, rd."14C-PP(mgCm-2d-1)" ::decimal  as value
, qua.quality_id as quality_id
-- , null as geom
-- select count(*)
from import.pml_primprod rd
left join nwdm.dataset ds on ds.data_holder='PML' and lower(ds.dataset_name) = replace(lower(rd.bestand), '.xlsx','')
left join import.mapping_primaryproduction m on lower(m.bestand)=lower(rd.bestand)
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D99'		-- = NA
left join import.mapping_matrix mmx on mmx.compartiment_code = 'OW'		--rd.compartiment_code
-- left join nwdm.matrix mat on mat.code = mmx.sdn_code
left join import.mapping_p06_p01 mpp on mpp.p01code=m.p01
left join nwdm.unit un on un.code = mpp.p06code
join nwdm.location loc on loc.location_code='pml_'|| rd._recordnr and loc.data_owner='PML'
/*left*/ join nwdm.parameter par01 on par01.code = m.p01
where rd."14C-PP(mgCm-2d-1)" is not null
;
