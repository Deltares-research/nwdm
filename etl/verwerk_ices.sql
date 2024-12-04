select count(*) from import.ices

--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('ICES',11) on conflict do nothing ;

--1. administration of source
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 700000 dataset_id
, 'table_name' as dataset_name
, null short_filename
, null "_path"
, null as file
, null number_of_records
, 'ICES' data_holder
, 'ICES' data_owner
, 'https://www.ices.dk/data' link_to_data
, 'https://gis.ices.dk/geonetwork/srv/eng/catalog.search#/search?facet.q=type%2Fdataset%26orgName%2FICES' link_to_metadata


--11. ices locations
delete from nwdm.location where data_owner='ICES';
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
     'ices_'||cruise||'_'||(row_number() over ())::text as location_code  -- check if this needs to be a unique number
    , coalesce(station, 'station'||cruise::varchar) as location_name
    , (rd.lon)::decimal as x
    , (rd.lat)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(lon,lat), 4326::int) as geom
    , 'ICES' data_owner
    from (	select distinct cruise, station, lat, lon
            from import.ices
    ) rd
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);


--
--
--
-- -- ices data
-- insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
-- select
-- rd._recordnr as recordnr_dataset
-- , null::int as recordvolgnr_dataset
-- , ds.dataset_id
-- , loc.location_id
-- , rd.date::date as "date"
-- -- , null as begin_time
-- -- , null as end_time
-- , null as depth
-- , vr.vertical_reference_id
-- -- , mat.matrix_id as matrix_id
-- , par01.parameter_id as parameter_id
-- , un.unit_id as unit_id
-- , rd."14C-PP(mgCm-2d-1)" ::decimal  as value
-- , qua.quality_id as quality_id
-- -- , null as geom
-- -- select count(*)
-- from import.pml_primprod rd
-- left join nwdm.dataset ds on ds.data_holder='PML' and lower(ds.dataset_name) = replace(lower(rd.bestand), '.xlsx','')
-- left join import.mapping_primaryproduction m on lower(m.bestand)=lower(rd.bestand)
-- join nwdm.quality qua on qua.code = '0' and qua.use_data=true
-- left join nwdm.vertical_reference vr on vr.code = 'D99'		-- = NA
-- left join import.mapping_matrix mmx on mmx.compartiment_code = 'OW'		--rd.compartiment_code
-- left join import.mapping_p06_p01 mpp on mpp.p01code=m.p01
-- left join nwdm.unit un on un.code = mpp.p06code
-- join nwdm.location loc on loc.location_code='pml_'|| rd._recordnr and loc.data_owner='PML'
-- join nwdm.parameter par01 on par01.code = m.p01
-- ;
