-- count the number of the old measurements
select count(*) from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='Rijkswaterstaat'
; --1051800
-- create backup
select m.* into nwdm._backup_measurement_rws from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='Rijkswaterstaat';
select d.* into nwdm._backup_dataset_rws from nwdm.dataset d where d.data_owner ='Rijkswaterstaat';
select l.* into nwdm._backup_location_rws from nwdm.location l  where l.data_owner ='Rijkswaterstaat';



--0. data_owners - during the update this line of code should not be run again.
insert into nwdm.data_owner (data_owner,priority) values ('Rijkswaterstaat',1) on conflict do nothing ;


--1. dataset nwdm rws
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select row_number() over (order by _path,"_short_filename") dataset_id --TODO: what is the shortfilename?
, replace(_short_filename, '.csv', '') as dataset_name --TODO: what is shortfilename
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'Rijkswaterstaat' data_holder
, 'Rijkswaterstaat' data_owner
, 'https://repos.deltares.nl/repos/Wozep/trunk/NWDM/nioz_data' link_to_data --TODO: add the correct link to data
, 'https://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/adw8xaji-ifam-4wnx-pu8g-oszpnerke8yo' link_to_metadata --TODO: add the correct link to metadata
from import.rawdata
group by "_short_filename", "_path"
;

--1b. dataset ddlpy
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
insert into nwdm.location(location_code, location_name,x,y,epsg,geom, data_owner)
select *
from (
    select distinct
     'ddl_'|| rd.code as location_code
    , rd.naam as location_name
    , rd.x::decimal as x	-- zie onder tbv transform
    , rd.y::decimal as y	-- zie onder tbv transform
    , rd.coordinatenstelsel::int as epsg	-- zie onder tbv transform
    , st_transform(st_setsrid(st_makepoint(rd.x::decimal, rd.y::decimal), rd.coordinatenstelsel::int), 4326) as geom
    , ds.data_owner
    from import.rwsddlpydata rd
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

--1b. observations ddlpy
insert into nwdm.measurement (recordnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd."_recordnr" as recordnr_dataset		
, ds.dataset_id as dataset_id
, loc.location_id
, rd.datetime::timestamp as "date"
,case when waarnemingmetadata_bemonsteringshoogtelijst::decimal <> -999999999 then waarnemingmetadata_bemonsteringshoogtelijst::decimal/-100.0 end as depth     -- rws meet altijd in cm, wij hanteren meter (bij sea level: groter is dieper)
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
,case when meetwaarde_waarde_numeriek  in ('NA','999999999999','-9999') then null::decimal else meetwaarde_waarde_numeriek ::decimal end as value
, qua.quality_id as quality_id
-- select *		-- select count(*)			--1426661
from import.rwsddlpydata rd
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on loc.location_code = rd.code and loc.data_owner='Rijkswaterstaat'
left join import.mapping_sdn_l201 map201 on map201.aquo_kwaliteitsoordeel_code = rd.waarnemingmetadata_kwaliteitswaardecodelijst
join nwdm.quality qua on qua.code = map201.sdn_l201_code and qua.use_data=true
left join nwdm.vertical_reference vr on vr.source_code = rd.waarnemingmetadata_referentievlaklijst
left join import.mapping_unit mun on mun.eenheid_code = rd.eenheid_code  and coalesce(mun.grootheid_code,rd.grootheid_code) = rd.grootheid_code
left join nwdm.unit un on un.code = mun.sdn_code
left join import.mapping_sdn_p01 map01
	on map01.aquo_grootheid_code = rd.grootheid_code
	and coalesce(map01.aquo_chemischestof_code, 'NVT') = rd.parameter_code
	and map01.aquo_hoedanigheid_code = rd.hoedanigheid_code
--	and map01.aquo_biotaxon_code = rd.biotaxon_code		-- nooit anders dan "NVT"
	and map01.aquo_compartiment_code = rd.compartiment_code
join nwdm.parameter par01 on par01.code = map01.sdn_p01_code
where (meetwaarde_waarde_numeriek  not in ('NA','999999999999','-9999','-99') and meetwaarde_waarde_numeriek  is not null)
;
