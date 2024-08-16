--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('Macovei',11) on conflict do nothing ;


-- dataset
-- delete from nwdm.dataset where data_owner='Macovei';
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 800000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.tab', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'Macovei' data_holder
, 'Macovei' data_owner
, 'https://doi.pangaea.de/10.1594/PANGAEA.930383' link_to_data
, 'https://doi.pangaea.de/10.1594/PANGAEA.930383' link_to_metadata
from import.macovei a
group by "_short_filename", "_path";

-- tijdelijke tabel tbv scope-beperking geom
drop table if exists import._temp_macovei_geom;
select _recordnr
, st_setsrid(st_makepoint(sub.x,sub.y),4326) as geom
, null::bool in_scope_north_sea
into import._temp_macovei_geom
from (
     select _recordnr
          , longitude::numeric as x
          , latitude::numeric as y
     from import.macovei
) sub
;

-- bepaal geom in scope
update import._temp_macovei_geom tmg set in_scope_north_sea = st_contains((select geom from nwdm.scope_northsea), tmg.geom);

---  measurement
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id,geom)
select
src._recordnr as recordnr_dataset
, src.recordvolgnr_dataset
, ds.dataset_id
, src."date_time"::timestamp as "date"
, null::decimal as depth
, vr.vertical_reference_id
, par.parameter_id as parameter_id
, un.unit_id as unit_id
, cvalue::decimal  as value
, qua.quality_id as quality_id
, tmg.geom as geom
-- select count(*)
from (
    select *
    from (
        select unnest(array [1,2,3]) as recordvolgnr_dataset
        , unnest(array ['pco2water_sst_wet_uatm','temp_c','sal']) as cname
        , nullif(replace(
                unnest(array ["pco2water_sst_wet_uatm","temp_c","sal"]),
                'NaN', ''),
             '') as cvalue
        , "date_time"
        , "latitude"
        , "longitude"
        , _recordnr
        , _short_filename
        from import.macovei
    ) sub where sub.cvalue is not null
) src
join import._temp_macovei_geom tmg on tmg._recordnr=src._recordnr
left join nwdm.dataset ds on ds.data_owner='Macovei' and lower(ds.short_filename) = lower(src._short_filename)
left join import.mapping_macovei m on lower(m.column_name)=lower(src.cname)
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = m.p06
join nwdm.parameter par on par.code = m.p01
;
