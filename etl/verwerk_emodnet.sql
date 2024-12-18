insert into nwdm.data_owner (data_owner,priority) values ('EMODnet',8) on conflict (data_owner) do nothing;

-- delete from nwdm.dataset where dataset_id in (1000001,1000002,1000003,1000004);
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 1000002 dataset_id
, 'EMODnet' as dataset_name
,'Eutrophication_NorthSea_non-nutrient_timeseries_2022_unrestricted.txt' short_filename
, 'S:\NWDM\emodnet_data\' "path"
, 'Eutrophication_NorthSea_non-nutrient_timeseries_2022_unrestricted.txt' as file
, null::int number_of_records
, 'EMODnet' data_holder
, 'EMODnet' data_owner
, '...' link_to_data
, 'https://emodnet.eu/' link_to_metadata
;
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 1000004 dataset_id
, 'EMODnet' as dataset_name
,'Eutrophication_NorthSea_nutrient_timeseries_2022_unrestricted.txt' short_filename
, 'S:\NWDM\emodnet_data\' "path"
, 'Eutrophication_NorthSea_nutrient_timeseries_2022_unrestricted.txt' as file
, null::int number_of_records
, 'EMODnet' data_holder
, 'EMODnet' data_owner
, '...' link_to_data
, 'https://emodnet.eu/' link_to_metadata
;

-- delete from nwdm.location where data_owner='EMODnet';
-- part 1
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
    --  sub.station || '_' ||row_number() over (order by station, x,y)::varchar as location_code
    'emodnet_'||min_mainid as location_code      -- NB location_name is niet uniek (alleen in combinatie met lon-lat)
    , sub.station as location_name
    , (sub.x)::decimal as x
    , (sub.y)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(x,y), 4326::int) as geom
    , 'EMODnet' data_owner
    from (
            select min(_mainid) min_mainid, station, x, y
            from (
                select _mainid _mainid, "Station" station, "Longitude [degrees_east]"::decimal x, "Latitude [degrees_north]"::decimal y from import.odv_sample2
                union all
                select _mainid+400000 _mainid, "Station" station, "Longitude [degrees_east]"::decimal x, "Latitude [degrees_north]"::decimal y from import.odv_sample4
            ) odvs
            group by station, x, y
    ) sub
)g
where st_contains((select geom from nwdm.scope_northsea),g.geom);
-- update nwdm.location set scope_northsea=st_contains((select geom from nwdm.scope_northsea), nwdm.location.geom);

-- insert measurement voorbereiden:
-- datum+tijd samenvoegen incl location_id
drop table if exists nwdm._temp_emodnet_sample2;
select _mainid
, case when date_time <> begin_time and date_time='00:00:00'::time then date_orig + begin_time else date_orig end as date
, loc.location_id
, sam."Depth reference" as depth_ref
into nwdm._temp_emodnet_sample2
from (
    select
    "yyyy-mm-ddThh:mm:ss.sss"::timestamp as date_orig, "yyyy-mm-ddThh:mm:ss.sss"::timestamp::time as date_time, "Start time"::time as begin_time
    , *
    from import.odv_sample2 odvs
) sam
join nwdm.location loc on loc.location_name = sam."Station" and loc.x = sam."Longitude [degrees_east]" and loc.y = sam."Latitude [degrees_north]" and loc.data_owner = 'EMODnet'
;
drop table if exists nwdm._temp_emodnet_sample4;
select _mainid  --7151
, case when date_time <> begin_time and date_time='00:00:00'::time then date_orig + begin_time else date_orig end as date
, loc.location_id
, sam."Depth reference" as depth_ref
into nwdm._temp_emodnet_sample4
from (
    select
    "yyyy-mm-ddThh:mm:ss.sss"::timestamp as date_orig, "yyyy-mm-ddThh:mm:ss.sss"::timestamp::time as date_time, "Start time"::time as begin_time
    , *
    from import.odv_sample4 odvs
) sam
join nwdm.location loc on loc.location_name = sam."Station" and loc.x = sam."Longitude [degrees_east]" and loc.y = sam."Latitude [degrees_north]" and loc.data_owner = 'EMODnet'
;

-- index odv-tabel
create index if not exists ix_odv_obs on import.odv_observation2 (_mainid, _subid, _varid) include (value);
create index if not exists ix_temp_emodnet_sample on nwdm._temp_emodnet_sample2 (_mainid) include (date,location_id);
create index if not exists ix_odv_obs on import.odv_observation4 (_mainid, _subid, _varid) include (value);
create index if not exists ix_temp_emodnet_sample on nwdm._temp_emodnet_sample4 (_mainid) include (date,location_id);

-- _nr bepalen tbv weglaten dubbele metingen
drop table if exists nwdm._temp_emodnet_observation2;
select obsd._mainid, obsd._subid
     , obsd.value as depth, obsd.quality as depth_quality, obs_date.value as date, tes.location_id, obs._varid, obs.value, obs.quality, tes.depth_ref
    , row_number() over (partition by obsd.value, obs_date.value, tes.location_id, obs._varid, obs.value order by obs._mainid, obs._subid,obs._varid) as _nr
into nwdm._temp_emodnet_observation2
from (select * from import.odv_observation2 where _varid=(select _varid from import.odv_variable2 where variable='Depth [m]') ) obsd
join (select * from import.odv_observation2 where _varid=(select _varid from import.odv_variable2 where variable='time_ISO8601')) obs_date on obsd._mainid=obs_date._mainid and obsd._subid = obs_date._subid
join (select * from import.odv_observation2 where _varid not in (select _varid from import.odv_variable2 where variable in ('Depth [m]','time_ISO8601'))) obs on obs._mainid=obsd._mainid and obs._subid = obsd._subid
join nwdm._temp_emodnet_sample2 tes on tes._mainid=obsd._mainid
;
drop table if exists nwdm._temp_emodnet_observation4;
select obsd._mainid, obsd._subid
     , obsd.value as depth, obsd.quality as depth_quality, obs_date.value as date, tes.location_id, obs._varid, obs.value, obs.quality, tes.depth_ref
    , row_number() over (partition by obsd.value, obs_date.value, tes.location_id, obs._varid, obs.value order by obs._mainid, obs._subid,obs._varid) as _nr
into nwdm._temp_emodnet_observation4
from (select * from import.odv_observation4 where _varid=(select _varid from import.odv_variable4 where variable='Depth [m]') ) obsd
join (select * from import.odv_observation4 where _varid=(select _varid from import.odv_variable4 where variable='time_ISO8601')) obs_date on obsd._mainid=obs_date._mainid and obsd._subid = obs_date._subid
join (select * from import.odv_observation4 where _varid not in (select _varid from import.odv_variable4 where variable in ('Depth [m]','time_ISO8601'))) obs on obs._mainid=obsd._mainid and obs._subid = obsd._subid
join nwdm._temp_emodnet_sample4 tes on tes._mainid=obsd._mainid
;

-- insert measurement uitvoeren mbt temp-tabel
-- delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner='EMODnet');
insert into nwdm.measurement(recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", depth_quality_id, vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
obs._mainid as recordnr_dataset
, obs._subid as recordvolgnr_dataset
, 1000002 as dataset_id
, obs.location_id
, obs.date::timestamp
, obs.depth::numeric
, quad.quality_id as depth_quality_id
, vr.vertical_reference_id
, par.parameter_id
, un.unit_id as unit_id
, obs.value::numeric --* cu.multiplication_factor_to_preferred_unit::decimal
, qua.quality_id
-- select *		-- select count(*)			--15285920
from (select * from nwdm._temp_emodnet_observation2 where _nr=1) obs -- filter dubbele metingen
join import.mapping_emodnet map on map._varid = obs._varid  -- tbv unit p06
join nwdm.parameter par on par.p35code = map.p35 and par.parameter_origin='p35'     -- variabelen zonder gemapte parameter vallen terecht weg (2x geaggregeerde variabele)
-- join nwdm.dataset ds on ds.dataset_name='EMODnet'
join nwdm.quality qua on qua.code = obs.quality::varchar and qua.use_data=true
join nwdm.quality quad on quad.code = obs.depth_quality::varchar and quad.use_data=true
left join nwdm.vertical_reference vr on vr.code = left(right(obs.depth_ref,4),3)
left join nwdm.unit un on un.code = map.p06
;

insert into nwdm.measurement(recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", depth_quality_id, vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
obs._mainid as recordnr_dataset
, obs._subid as recordvolgnr_dataset
, 1000004 as dataset_id
, obs.location_id
, obs.date::timestamp
, obs.depth::numeric
, quad.quality_id as depth_quality_id
, vr.vertical_reference_id
, par.parameter_id
, un.unit_id as unit_id
, obs.value::numeric --* cu.multiplication_factor_to_preferred_unit::decimal
, qua.quality_id
-- select *		-- select count(*)			--15285920
from (select * from nwdm._temp_emodnet_observation4 where _nr=1) obs -- filter dubbele metingen
join import.mapping_emodnet map on map._varid = obs._varid  -- tbv unit p06
join nwdm.parameter par on par.p35code = map.p35 and par.parameter_origin='p35'     -- variabelen zonder gemapte parameter vallen terecht weg (2x geaggregeerde variabele)
-- join nwdm.dataset ds on ds.dataset_name='EMODnet'
join nwdm.quality qua on qua.code = obs.quality::varchar and qua.use_data=true
join nwdm.quality quad on quad.code = obs.depth_quality::varchar and quad.use_data=true
left join nwdm.vertical_reference vr on vr.code = left(right(obs.depth_ref,4),3)
left join nwdm.unit un on un.code = map.p06
;

-- create index if not exists ix_location_emodnet_id on nwdm.measurement_emodnet(location_emodnet_id);
