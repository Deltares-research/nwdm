--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('Rijkswaterstaat',1) on conflict do nothing ;


--1. dataset nwdm thredds
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'Rijkswaterstaat' data_holder
, 'Rijkswaterstaat' data_owner
, case 	when _path ilike '%noordzee%' then 'https://watersysteemdata.deltares.nl/thredds/catalog/watersysteemdata/Noordzee/ddl/raw/catalog.html'
		when _path ilike '%wadden%' then 'https://watersysteemdata.deltares.nl/thredds/catalog/watersysteemdata/Wadden/ddl/raw/catalog.html'
		end link_to_data
, 'https://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/adw8xaji-ifam-4wnx-pu8g-oszpnerke8yo' link_to_metadata
from import.rawdata
group by "_short_filename", "_path"
;
--1b. dataset donar
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 50000+row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'DONAR' data_holder
, 'Rijkswaterstaat' data_owner
, null link_to_data
, null link_to_metadata
from import.donar_sioene
group by "_short_filename", "_path"
;


--1.locations nwdm thredds (rws)
-- delete from nwdm.location;
insert into nwdm.location(location_code, location_name,x,y,epsg,geom, data_owner)
select *
from (
    select distinct
     rd.locatie_code as location_code
    , rd.locatie_naam as location_name
    , rd.geometriepunt_x::decimal as x	-- zie onder tbv transform
    , rd.geometriepunt_y::decimal as y	-- zie onder tbv transform
    , rd.coordinatenstelsel::int as epsg	-- zie onder tbv transform
    , st_transform(st_setsrid(st_makepoint(rd.geometriepunt_x::decimal, rd.geometriepunt_y::decimal), rd.coordinatenstelsel::int), 4326) as geom
    , ds.data_owner
    from import.rawdata rd
    join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;
--1.locations donar
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select distinct
    -- rd.gbd || '_' || dense_rank() over (partition by rd.gbd order by rd.loc_x,rd.loc_y)::varchar as location_code
    'donar_'|| rd.loc as location_code
    , rd.loc as location_name
    , st_x(rd.geom)::decimal as x	-- zie onder tbv transform
    , st_y(rd.geom)::decimal as y	-- zie onder tbv transform
    , st_srid(rd.geom)::int as epsg	-- zie onder tbv transform
    , rd.geom
    , ds.data_owner
    from (select *
        , st_transform(st_setsrid(st_makepoint(loc_x::decimal/100, loc_y::decimal/100), 28992::int), 4326) as geom
        from import.donar_sioene
        ) rd
    join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;
-- transformation verwerken in x/y/epsg:
update nwdm.location  set x=st_x(geom), y=st_y(geom), epsg=st_srid(geom) ;



--1a. OBSERVATIONS NWDM VAN THREDDS SERVER - RWS
--truncate table nwdm.measurement;
insert into nwdm.measurement (recordnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd."_recordnr" as recordnr_dataset		-- TO DO: rename...?
, ds.dataset_id as dataset_id
, loc.location_id
, rd.tijdstip::timestamp as "date"
,case when bemonsteringshoogte::decimal <> -999999999 then bemonsteringshoogte::decimal/-100.0 end as depth     -- rws meet altijd in cm, wij hanteren meter (bij sea level: groter is dieper)
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
,case when numeriekewaarde in ('NA','999999999999','-9999') then null::decimal else numeriekewaarde::decimal end as value
, qua.quality_id as quality_id
-- select *		-- select count(*)			--1426661
from import.rawdata rd
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on loc.location_code = rd.locatie_code and loc.data_owner='Rijkswaterstaat'
left join import.mapping_sdn_l201 map201 on map201.aquo_kwaliteitsoordeel_code = rd.kwaliteitswaarde_code
join nwdm.quality qua on qua.code = map201.sdn_l201_code and qua.use_data=true
left join nwdm.vertical_reference vr on vr.source_code = rd.referentievlak
left join import.mapping_unit mun on mun.eenheid_code = rd.eenheid_code and coalesce(mun.grootheid_code,rd.grootheid_code) = rd.grootheid_code
left join nwdm.unit un on un.code = mun.sdn_code
left join import.mapping_sdn_p01 map01
	on map01.aquo_grootheid_code = rd.grootheid_code
	and coalesce(map01.aquo_chemischestof_code, 'NVT') = rd.parameter_code
	and map01.aquo_hoedanigheid_code = rd.hoedanigheid_code
--	and map01.aquo_biotaxon_code = rd.biotaxon_code		-- nooit anders dan "NVT"
	and map01.aquo_compartiment_code = rd.compartiment_code
join nwdm.parameter par01 on par01.code = map01.sdn_p01_code
where (numeriekewaarde not in ('NA','999999999999','-9999','-99') and numeriekewaarde is not null)
;
--1b. observations DONAR
insert into nwdm.measurement (recordnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd."_recordnr" as recordnr_dataset		-- TO DO: rename...?
, ds.dataset_id as dataset_id
, loc.location_id
,(rd.datum::date + replace(replace(rd.tijd,'''',''),'T',' ')::time)::timestamp as "date"
,case when rd.plt_bmh::decimal <> -999999999 then rd.plt_bmh::decimal/-100.0 end as depth     -- rws meet altijd in cm, wij hanteren meter (bij sea level: groter is dieper)
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
,case when rd.waarde in ('NA','999999999999') then null::decimal else rd.waarde::decimal end as value
, qua.quality_id as quality_id
--select count(*)			--77391
from import.donar_sioene rd
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on loc.location_code = 'donar_'||rd.loc and loc.data_owner='Rijkswaterstaat'
left join import.mapping_sdn_l201 map201 on map201.aquo_kwaliteitsoordeel_code = rd.kwc		--KWC
join nwdm.quality qua on qua.code = map201.sdn_l201_code and qua.use_data=true
left join nwdm.vertical_reference vr on vr.source_code = rd.plt_refvlak
left join import.mapping_unit mun on mun.eenheid_code = rd.ehd --and coalesce(mun.grootheid_code,rd.grootheid_code) = rd.grootheid_code		-- EHD , geen grootheid
left join nwdm.unit un on un.code = mun.sdn_code
left join import.mapping_sdn_p01 map01 	-- --> RXXXPARK
	on coalesce(map01.donar_par, 'NVT') = rd.par		--SiO2
	and map01.donar_hdh = rd.hdh			--Sinf
	and map01.donar_cpm = rd.cpm			-- 10
join nwdm.parameter par01 on par01.code = map01.sdn_p01_code
where (rd.waarde not in ('NA','999999999999', '-99') and rd.waarde is  not null)
;

