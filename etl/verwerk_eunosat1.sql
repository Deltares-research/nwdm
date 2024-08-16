--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('Schlesswig-Holstein',2)
,('RBINS',6)
,('Danish MSFD database',6)
,('CEFAS',6)
,('IFREMER',6)
,('IMR',6)
,('MSS',6)
,('SMHI',6)
on conflict do nothing ;


--2a. dataset eunosat
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_holder in ('EUNOSAT', 'INTERREG'));
delete from nwdm.dataset where data_holder in ('EUNOSAT', 'INTERREG');
-- delete from nwdm.dataset where data_holder = 'INTERREG';
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
with m as (select * from import.mapping_eunosat_sdn)
select 100000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'EUNOSAT' data_holder
, case
    when _short_filename = 'BE.csv' then 'RBINS'
    when _short_filename = 'DK.csv' then 'Danish MSFD database'
    when _short_filename = 'EN.csv' then 'CEFAS'
    when _short_filename = 'FR.csv' then 'IFREMER'
    when _short_filename = 'GE.csv' then 'Schlesswig-Holstein'
    when _short_filename = 'NO.csv' then 'IMR'
    when _short_filename = 'SC.csv' then 'MSS'
    when _short_filename = 'SE.csv' then 'SMHI'
    end data_owner
, (select additional from m where ltrim(csv_name) = 'link_to_data') link_to_data
, (select additional from m where ltrim(csv_name) = 'link_to_metadata') link_to_metadata
from import.eunosat eun
group by "_short_filename", "_path";

--2b. aanvullende eunosat data schlesswig-holstein:
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 100100 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'INTERREG' data_holder        --!!??
, 'Schlesswig-Holstein' data_owner
, 'p:\11200300-002-jmp-eunosat\validatiedata\daily_data\' link_to_data
, 'p:\11200300-002-jmp-eunosat\validatiedata\daily_data\' link_to_metadata
from import.eunosat_sh eun
group by "_short_filename", "_path";

--2a.eunosat locations
delete from nwdm.location where location_code like 'eu_%';
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select distinct
     'eu_'||rd.location_name as location_code
    , rd.location_name as location_name
    , (rd.longitude)::decimal as x
    , (rd.latitude)::decimal as y
    , m1.epsg::int as epsg
    , st_setsrid(st_makepoint(rd.longitude::decimal, rd.latitude::decimal), 4326::int) as geom
    , ds.data_owner -- select *
    from (select *, row_number() over (partition by location_name order by location_name, latitude, longitude) _nr from import.eunosat) rd
    join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename" and ds.dataset_id>100000 and rd._nr=1
    join (select additional::int as epsg from import.mapping_eunosat_sdn where csv_name = 'EPSG') m1 on true
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;
-- 2b. eunosat locatie schlesswig-holstein: aanvulling (bron: JMP-EUNOSAT_coordinates_lat-lon.xlsx)
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner,station)
select *
from (
    select 'eu_Au'||chr(223)||'eneider, Tonne 29' as location_code       -- ringel-S = ß = chr(223)
    , 'Au'||chr(223)||'eneider, Tonne 29' as location_name
    ,8.6917::decimal as x
    ,54.265::decimal as y
    , 4326 as epsg
    , st_setsrid(st_makepoint(8.6917::decimal, 54.265::decimal), 4326::int) as geom
    , 'Schlesswig-Holstein' data_owner
    ,'Ausseneider_Tonne29' as station
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;

