--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('VLIZ',10) on conflict do nothing ;


-- 10. VLIZ-dataset
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 600000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'VLIZ' data_holder
, 'VLIZ' data_owner
-- , 'https://repos.deltares.nl/repos/Wozep/trunk/NWDM/vliz_data' link_to_data
, 'https://rshiny.lifewatch.be/station-data/' link_to_data
, 'https://doi.org/10.14284/441 Flanders Marine Institute (VLIZ), Belgium (2021): LifeWatch observatory data: nutrient, pigment, suspended matter and secchi measurements in the Belgian Part of the North Sea.' link_to_metadata
from import.vliz_station_abiotic fh
group by "_short_filename", "_path";


-- 10. vliz locations
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
    station as location_code
    , src.station::varchar as location_name
    , src.x
    , src.y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(x,y), 4326::int) as geom
    , 'VLIZ' data_owner
    from (
        select *
        , longitude::decimal as x
        , latitude::decimal as y
        from (
            select *
            , row_number() over (partition by station order by aantal desc) _nr
            from (
                     select station, longitude, latitude, count(*) aantal
                     from import.vliz_station_abiotic
                     group by station, longitude, latitude
            ) sub
        ) sub1
        where _nr=1
    ) src
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;


--- VLIZ measurement
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
src._recordnr as recordnr_dataset
, null::int as recordvolgnr_dataset
, ds.dataset_id
, loc.location_id
, src."Time"::date as "date"
-- , src."Time"::time as begin_time
-- , null as end_time
, case when src."Pressure(db)"<>'NA' then src."Pressure(db)" end::decimal as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, cvalue::decimal  as value
, qua.quality_id as quality_id
-- , null as geom
-- select count(*)
-- from import.vliz_station_abiotic src
from (
    select *
    from (
        select unnest(array [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19])
        , unnest(array ['Ammonium_NH4(umol_N_NH4/L)','cpar','Density(kg/m3)','Fluorescence(mg/m3)','Nitrate_Nitrite(umol_N_NO3_NO2/L)','Nitrate_NO3(umol_N_NO3/L)','Nitrite_NO2(umol_N_NO2/L)','OBS(NTU)','par','ph','Phosphate_PO4(umol_P_PO4/L)','Pressure(db)','Salinity(PSU)','Secchi_Depth(cm)','Silicate_SiO4(umol_Si_SiO4/L)','Sound_Velocity(m/s)','spar','SPM(mg/l)','Temperature(degC)']) as cname
        , nullif(replace(
                unnest(array ["Ammonium_NH4(umol_N_NH4/L)","cpar","Density(kg/m3)","Fluorescence(mg/m3)","Nitrate_Nitrite(umol_N_NO3_NO2/L)","Nitrate_NO3(umol_N_NO3/L)","Nitrite_NO2(umol_N_NO2/L)","OBS(NTU)","par","ph","Phosphate_PO4(umol_P_PO4/L)","Pressure(db)","Salinity(PSU)","Secchi_Depth(cm)","Silicate_SiO4(umol_Si_SiO4/L)","Sound_Velocity(m/s)","spar","SPM(mg/l)","Temperature(degC)"]),
                'NA', ''),
             '') as cvalue
        , "station"
        , "Time"
        , "latitude"
        , "longitude"
        , "Pressure(db)"
        , _recordnr
        , _short_filename
        from import.vliz_station_abiotic
    ) sub where sub.cvalue is not null
) src
left join nwdm.dataset ds on ds.data_holder='VLIZ' and lower(ds.short_filename) = src._short_filename
left join import.mapping_vliz_abiotic m on lower(m.vliz_abiotic_parameter)=lower(src.cname)
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'UKN'		--src.referentievlak
left join import.mapping_matrix mmx on mmx.compartiment_code = 'OW'		--src.compartiment_code
-- left join nwdm.matrix mat on mat.code = mmx.sdn_code
left join import.mapping_p06_p01 mpp on mpp.p01code=m.p01
left join nwdm.unit un on un.code = mpp.p06code
join nwdm.location loc on loc.location_code=src.station and loc.data_owner='VLIZ'
/*left*/ join nwdm.parameter par01 on par01.code = m.p01
;
