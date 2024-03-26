create extension if not exists postgis;
create extension if not exists unaccent;
drop schema if exists nwdm cascade;
create schema if not exists nwdm;

create or replace view nwdm.scope_northsea as
SELECT st_setsrid(ST_MakePolygon( ST_GeomFromText('LINESTRING(-1.5304392E+01 6.4098841E+01,-1.4410876E+01 6.4067820E+01,-1.0759481E+01 6.4070439E+01,-5.6766278E+00 6.4070769E+01,-2.2041645E+00 6.4077688E+01,1.9135725E+00 6.4071763E+01,5.8440204E+00 6.4074696E+01,9.6151452E+00 6.4073397E+01,1.3278998E+01 5.6642482E+01,1.3278998E+01 5.4326969E+01,-1.8516047E+00 4.7878294E+01,-3.2125148E-01 4.5092610E+01,-1.4536378E+00 4.3166853E+01,-9.2823806E+00 4.2909005E+01,-1.5155557E+01 4.2875958E+01,-1.5304392E+01 6.4098841E+01)')),4326) as geom
;

-- create or replace view nwdm.scope_waddensea as
-- SELECT ST_MakePolygon( ST_GeomFromText('LINESTRING(4.774654 52.815750, 4.671893 52.922283, 4.686977 53.094810, 4.791624 53.232456, 4.968883 53.352207, 5.115012 53.409718, 5.328077 53.448372, 5.527000 53.499283, 5.636381 53.510612, 5.728772 53.488930, 5.871130 53.487988, 6.005945 53.525696, 6.104936 53.540783, 6.259550 53.543610, 6.367025 53.584148, 6.447160 53.640717, 6.599893 53.677490, 6.741308 53.700115, 6.937404 53.711430, 7.059021 53.728397, 7.126900 53.748196, 7.281518 53.755741, 7.421990 53.782139, 7.553978 53.791569, 7.621857 53.811367, 7.780242 53.815136, 7.909426 53.814217, 8.086666 53.914150, 8.257306 53.977318, 8.440204 54.123447, 8.445875 54.281849, 8.358197 54.409122, 8.250722 54.539223, 8.174358 54.663670, 8.236580 54.757004, 8.301632 54.909733, 8.338400 55.099232, 8.352541 55.280243, 8.324334 55.378365, 8.224401 55.444359, 8.087700 55.510353, 8.248913 55.654598, 8.617535 55.603687, 8.807045 55.365181, 9.098360 54.808002, 9.208664 54.508202, 9.146461 54.261211, 9.634829 53.829437, 9.996851 53.552265, 9.637656 53.187412, 8.673206 52.997913, 7.389157 53.040340, 6.297448 53.014896, 5.791182 52.833885, 4.989872 52.786785, 4.774654 52.815750)')) as geom;

drop table if exists nwdm.data_owner cascade;
create table nwdm.data_owner
(
    data_owner varchar primary key
    ,priority smallint not null
);

drop table if exists nwdm.dataset cascade;
create table nwdm.dataset(
dataset_id int primary key
,dataset_name varchar
,short_filename varchar
,path varchar
,file varchar
,number_of_records int
,data_holder varchar
,data_owner varchar NOT NULL references nwdm.data_owner(data_owner) on delete cascade
,link_to_data varchar
,link_to_metadata varchar
, creation_date timestamp default now()
);

drop table if exists nwdm.modelgrid cascade;
create table nwdm.modelgrid(
    modelgrid_id serial primary key
--     , src int
--     , gid int
    , geom geometry
);

drop table if exists nwdm.grid cascade;
create table nwdm.grid(
    grid_id serial primary key
--     , i int
--     , j int
    , geom geometry
);

drop table if exists nwdm.location cascade;
create table nwdm.location (
	location_id serial primary key
	,location_code varchar not null
	,location_name varchar
	,x decimal
	,y decimal
	,epsg int
	,geom geometry
	,number_of_observations int
	,first_year smallint
	,last_year smallint
    ,data_owner varchar NOT NULL references nwdm.data_owner(data_owner) on delete cascade
--     ,scope_northsea bool
    ,modelgrid_id int references nwdm.modelgrid(modelgrid_id) on delete cascade
    ,grid_id int references nwdm.grid(grid_id) on delete cascade
    ,station varchar
    ,constraint c_location_code_data_owner unique (location_code, data_owner)
);
create index if not exists ix_location_geom on nwdm.location using gist(geom);

drop table if exists nwdm.parameter cascade;
create table nwdm.parameter (
	parameter_id int primary key
	, code varchar unique
	, preflabel varchar
	, altlabel varchar
	, definition varchar
	, parameter_origin varchar
	, p35code varchar
	, p35preflabel varchar
	, p35altlabel varchar
	, p35definition varchar
);

drop table if exists nwdm.parameter_emodnet cascade;
create table nwdm.parameter_emodnet (
	parameter_emodnet_id int primary key
	, code varchar unique
	, label varchar
	, altlabel varchar
	, definition varchar
);

drop table if exists nwdm.unit cascade;
create table nwdm.unit (
	unit_id smallint primary key
	, code varchar unique
	, preflabel varchar
	, altlabel varchar
	, definition varchar
);

drop table if exists nwdm.quality cascade;
create table nwdm.quality (
	quality_id smallint primary key
	, code varchar unique
	, preflabel varchar
	, altlabel varchar
	, definition varchar
    , use_data bool
);

drop table if exists nwdm.vertical_reference cascade;
create table nwdm.vertical_reference (
	vertical_reference_id smallint primary key
	, code varchar unique
	, source_code varchar
	, sdn_code varchar
	, preflabel varchar
	, altlabel varchar
	, definition varchar
);

drop table if exists nwdm.measurement cascade;
create table nwdm.measurement(
 measurement_id serial primary key
, recordnr_dataset int --uniek icm recordvolgnr_dataset
, recordvolgnr_dataset int -- volgnr bij splitsing van originele records
, dataset_id int not null references nwdm.dataset(dataset_id) on delete cascade
, location_id int null references nwdm.location(location_id) on delete cascade
, "date" timestamp
, depth decimal -- diepte in meter
, depth_quality_id smallint null references nwdm.quality(quality_id)
, vertical_reference_id smallint null references nwdm.vertical_reference(vertical_reference_id)
, parameter_id int null references nwdm.parameter(parameter_id)
, unit_id smallint null references nwdm.unit(unit_id)
, value decimal
, quality_id smallint null references nwdm.quality(quality_id)
, geom geometry         -- either a location_id OR this geom    -- alter table nwdm.measurement add column geom geometry;
, redundant bool not null default false
, modelgrid_id int references nwdm.modelgrid(modelgrid_id) on delete cascade
, grid_id int references nwdm.grid(grid_id) on delete cascade
-- , constraint c_recordnr_meas (recordnr_dataset,recordvolgnr_dataset)
);
-- alter table nwdm.measurement set (fillfactor =90);

drop view if exists nwdm.p35code cascade;
create view nwdm.p35code as 
select distinct p35code, p35preflabel, p35altlabel, p35definition from nwdm.parameter;

drop view if exists nwdm.measurement_p01_all cascade;
create or replace view nwdm.measurement_p01_all as
select m.measurement_id
, ds.data_holder, ds.data_owner, ds.dataset_name, ds.link_to_data, ds.link_to_metadata
, loc.location_code, loc.location_name
, coalesce(loc.geom, m.geom) as geom
, m."date"
, m."depth"
, quad.code as depth_quality_code, quad.preflabel as depth_quality_preflabel
, vr.code as vertical_reference_code, vr.preflabel as vertical_reference_preflabel
, par.code as parameter_code, par.preflabel as parameter_label
, un.code as unit_code, un.preflabel as unit_preflabel
, m.value
, qua.code as quality_code, qua.preflabel as quality_preflabel
, par.p35code, par.p35preflabel
, case when loc.location_id is not null then true else false end as fixed_location
, m.modelgrid_id
, m.grid_id
, loc.station
from nwdm.measurement m
left join nwdm.dataset ds on ds.dataset_id = m.dataset_id
left join nwdm.location loc on loc.location_id = m.location_id
left join nwdm.vertical_reference vr on vr.vertical_reference_id = m.vertical_reference_id
left join nwdm.parameter par on par.parameter_id = m.parameter_id
left join nwdm.unit un on un.unit_id = m.unit_id
left join nwdm.quality qua on qua.quality_id = m.quality_id
left join nwdm.quality quad on quad.quality_id = m.depth_quality_id
where m.redundant=false
;

drop view if exists nwdm.measurement_p01_fixed_locations cascade;
create or replace view nwdm.measurement_p01_fixed_locations as
select *
from nwdm.measurement_p01_all
where fixed_location = true
;

drop view if exists nwdm.measurement_interreg cascade;
create or replace view nwdm.measurement_interreg as
select *
from nwdm.measurement_p01_all
where (
    data_holder in ('DONAR','Rijkswaterstaat','INTERREG','EMODnet','SOCAT')
    or
    (data_holder='EUNOSAT' and dataset_name in ('FR','GE','NO','SC'))
    )
;

drop view if exists nwdm.measurement_jerico cascade;
create or replace view nwdm.measurement_jerico as
select *
from nwdm.measurement_p01_all
where (
    data_holder in ('DONAR','Rijkswaterstaat','NIOZ','HZG','EMODnet','SOCAT')
    or
    (data_holder='EUNOSAT' and dataset_name in ('FR','NO','SC','EN'))
    )
;

drop view if exists nwdm.measurement_hereon cascade;
create or replace view nwdm.measurement_hereon as
select *
from nwdm.measurement_p01_all
where (
    data_holder in ('DONAR','Rijkswaterstaat','NIOZ','HZG','EMODnet','SOCAT')
    or
    (data_holder='EUNOSAT' and dataset_name in ('FR','NO','SC','EN'))
    )
;

drop view if exists nwdm.measurement_p35_all;
create or replace view nwdm.measurement_p35_all as
select measurement_id as measurement_p35_all_id
, location_code
, location_name
, geom
, date
, depth
, vertical_reference_code
, vertical_reference_preflabel
, p35code
, p35preflabel
, unit_code
, unit_preflabel
, value
, quality_code
, quality_preflabel
, data_owner
, modelgrid_id
, grid_id
, station
from nwdm.measurement_p01_all
where p35code is not null
;

drop view if exists nwdm.p35overview;
create or replace view nwdm.p35overview as
select c.*, coalesce(m.count,0) as count
from nwdm.p35code c
left join (
    select p35code,count(*) as count
    from nwdm.measurement_p35_all
    group by p35code
) m on m.p35code=c.p35code
;

grant usage on schema nwdm to nwdm;
grant select ON ALL TABLES IN SCHEMA nwdm TO nwdm;
alter default privileges in schema nwdm grant select on tables to nwdm;
