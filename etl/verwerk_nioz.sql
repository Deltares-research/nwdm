-- 5. nioz data
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 400000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'NIOZ' data_holder
, 'NIOZ' data_owner
, 'https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg' link_to_data
, 'https://dataverse.nioz.nl/dataset.xhtml?persistentId=doi:10.25850/nioz/7b.b.kg' link_to_metadata
from import.nioz fh
group by "_short_filename", "_path";


--3.nioz locations
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
     'nioz_'||rd.station as location_code
    , rd.station as location_name
    , (rd.x)::decimal as x
    , (rd.y)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(x,y), 4326::int) as geom
    , 'NIOZ' data_owner
    from (	select distinct station, latitude, longitude
            , latitude::decimal y, longitude::decimal x
            from import.nioz
    ) rd
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;

select l.* from nwdm.location l  where l.data_owner ='NIOZ';



-- 5. nioz data
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id as location_id
, rd.datetime::date as "date"
, case when rd.depth='-999' then null else depth end ::numeric as depth
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, case when rd.cvalue = '-999' then null else replace(rd.cvalue, '<','') end ::decimal  as value
, qua.quality_id as quality_id
-- select count(*)
from (
    select
    cnr, cname, cvalue
    , case cname    when 'dic' then dic_flag
                    when 'alkalinity' then alkalinity_flag end as flag
                    
    ,station, bottle_id, datetime, depth_nominal, _recordnr, _short_filename, _path
    from (
         select unnest(array [1,2,3,4,5]) as cnr
              , unnest(array ['temperature','salinity','dic','alkalinity']) as cname
              , replace(unnest(array['temperature', 'salinity', 'dic', 'alkalinity']), ',', '.') as cvalue
 -- komma=duizendscheidingsteken
              , dic_flag
              , alkalinity_flag
              ,station, bottle_id, datetime, depth_nominal, _recordnr, _short_filename, _path
         from (select * from import.nioz ) sub
     )sub2
    where sub2.cvalue is not null and sub2.cvalue <> '-999'
) rd
left join import.mapping_nioz m on m.originalName = rd.cname
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on lower(loc.location_code) = 'nioz_'||lower(rd.station) and loc.data_owner='NIOZ'
join nwdm.quality qua on qua.code = rd.flag and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = m.p06
join nwdm.parameter par01 on par01.code = m.p01
;