insert into nwdm.data_owner (data_owner,priority) values ('EMODnet',8) on conflict (data_owner) do nothing;

insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 1000000 dataset_id
, 'EMODnet' as dataset_name
,'North_Sea_eutrophication_and_acidity_aggregated_v2018_2.txt' short_filename
, 'S:\NWDM\emodnet_data\' "path"
, 'North_Sea_eutrophication_and_acidity_aggregated_v2018_2.txt' as file
, null::int number_of_records
, 'EMODnet' data_holder
, 'EMODnet' data_owner
, 'https://repos.deltares.nl/repos/Wozep/trunk/NWDM/emodnet_data' link_to_data
, 'https://emodnet.eu/' link_to_metadata
;

-- delete from nwdm.location where data_owner='EMODnet';
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
    from (	select min(_mainid) min_mainid, station, longitude_degrees_east::decimal as x, latitude_degrees_north::decimal as y
            from import.odv_sample
            group by station, longitude_degrees_east::decimal, latitude_degrees_north::decimal
    ) sub
)g
where st_contains((select geom from nwdm.scope_northsea),g.geom);
-- update nwdm.location set scope_northsea=st_contains((select geom from nwdm.scope_northsea), nwdm.location.geom);

-- insert measurement voorbereiden:
-- datum+tijd samenvoegen incl location_id
drop table if exists nwdm._temp_emodnet_sample;
select _mainid  --186624
, case when date_time <> begin_time and date_time='00:00:00'::time then date_orig + begin_time else date_orig end as date
, loc.location_id
into nwdm._temp_emodnet_sample
from (
    select
    yyyy_mm_ddthh_mm_ss_sss::timestamp as date_orig, yyyy_mm_ddthh_mm_ss_sss::timestamp::time as date_time, start_time::time as begin_time
    , *
    from import.odv_sample
) sam
join nwdm.location loc on loc.location_name = sam.station and loc.x = sam.longitude_degrees_east and loc.y = sam.latitude_degrees_north and loc.data_owner = 'EMODnet'
;

-- index odv-tabel
create index if not exists ix_odv_obs on import.odv_observation (_mainid, _subid, _varid) include (value);
create index if not exists ix_temp_emodnet_sample on nwdm._temp_emodnet_sample (_mainid) include (date,location_id);

-- _nr bepalen tbv weglaten dubbele metingen
drop table if exists nwdm._temp_emodnet_observation;
select obs1._mainid, obs1._subid
     , obs1.value as depth, obs1.quality as depth_quality, tes.date, tes.location_id, obs._varid, obs.value, obs.quality
    , row_number() over (partition by coalesce(obs1.value,0), tes.date, tes.location_id, obs._varid, obs.value order by obs._mainid, obs._subid,obs._varid) as _nr
into nwdm._temp_emodnet_observation
from (select * from import.odv_observation where _varid=1) obs1
join (select * from import.odv_observation where _varid>1) obs on obs._mainid=obs1._mainid and obs._subid = obs1._subid
join nwdm._temp_emodnet_sample tes on tes._mainid=obs1._mainid
where obs._varid is not null and obs.value is not null
;

-- insert measurement uitvoeren mbt temp-tabel
-- delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner='EMODnet');
insert into nwdm.measurement(recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", depth_quality_id, vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
obs._mainid as recordnr_dataset
, obs._subid as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, obs.location_id
, obs.date
, obs.depth
, quad.quality_id as depth_quality_id
, vr.vertical_reference_id
, par.parameter_id
, un.unit_id as unit_id
, obs.value --* cu.multiplication_factor_to_preferred_unit::decimal
, qua.quality_id
-- select *		-- select count(*)			--15285920
from (select * from nwdm._temp_emodnet_observation where _nr=1) obs -- filter dubbele metingen
join import.mapping_emodnet map on map._varid = obs._varid  -- tbv unit p06
join nwdm.parameter par on par.p35code = map.p35 and par.parameter_origin='p35'     -- variabelen zonder gemapte parameter vallen terecht weg (2x geaggregeerde variabele)
join nwdm.dataset ds on ds.dataset_name='EMODnet'
join nwdm.quality qua on qua.code = obs.quality::varchar and qua.use_data=true
join nwdm.quality quad on quad.code = obs.depth_quality::varchar and quad.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = map.p06
-- join import.conversie_unit cu on cu.p35code=par.p35code and cu.unit_code=un.code and cu.multiplication_factor_to_preferred_unit is not null and cu.preferred_unit is not null
;

-- create index if not exists ix_location_emodnet_id on nwdm.measurement_emodnet(location_emodnet_id);
