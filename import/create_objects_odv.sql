
drop table if exists import.odv_variable cascade;
CREATE TABLE import.odv_variable
(
  _varid BIGINT
, "variable" TEXT
)
;

drop table if exists import.odv_sample cascade;
CREATE TABLE import.odv_sample
(
  _mainid int
, cruise TEXT
, station TEXT
, "type" TEXT
, yyyy_mm_ddthh_mm_ss_sss TEXT
, longitude_degrees_east DOUBLE PRECISION
, latitude_degrees_north DOUBLE PRECISION
, local_cdi_id TEXT
, edmo_code DOUBLE PRECISION
, bot__depth_m DOUBLE PRECISION
, instrument_info TEXT
, p01_codes TEXT
, "references" TEXT
, data_set_name TEXT
, discipline TEXT
, category TEXT
, variables_measured TEXT
, data_format TEXT
, data_format_version DOUBLE PRECISION
, data_size DOUBLE PRECISION
, data_set_creation_date DOUBLE PRECISION
, datum TEXT
, measuring_area_type TEXT
, water_depth_m DOUBLE PRECISION
, depth_reference TEXT
, minimum_instrument_depth_m DOUBLE PRECISION
, maximum_instrument_depth_m DOUBLE PRECISION
, start_date DOUBLE PRECISION
, start_time TEXT
, end_date DOUBLE PRECISION
, end_time TEXT
, vertical_resolution DOUBLE PRECISION
, vertical_resolution_unit TEXT
, instrument___gear_type TEXT
, track_resolution DOUBLE PRECISION
, track_resolution_unit DOUBLE PRECISION
, frequency DOUBLE PRECISION
, frequency_unit DOUBLE PRECISION
, platform_type TEXT
, cruise_name TEXT
, alternative_cruise_name TEXT
, cruise_start_date DOUBLE PRECISION
, station_name TEXT
, alternative_station_name TEXT
, station_start_date DOUBLE PRECISION
, originator TEXT
, data_holding_centre TEXT
, project_name TEXT
, edmed_references TEXT
, csr_references TEXT
, publication_references DOUBLE PRECISION
, data_distributor TEXT
, database_reference TEXT
, access_ordering_of_data TEXT
, access_restriction TEXT
, cdi_record_id DOUBLE PRECISION
, cdi_record_creation_date DOUBLE PRECISION
, cdi_partner TEXT
)
;

drop table if exists import.odv_observation cascade;
create table import.odv_observation
(
	_mainid bigint,
	_subid bigint,
	_varid bigint,
	value double precision,
	quality bigint
);
-- create index ix_odv_obs on import.odv_observation (_mainid, _subid, _varid);

drop table if exists import.mapping_emodnet;
CREATE TABLE import.mapping_emodnet
(
  _varid int
, "variable" TEXT
, p01 TEXT
, p35 TEXT
, p06 TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

-- select ',"'||column_name||'" as '||lower(replace(replace(replace(replace(replace(replace(replace(column_name, ':','_'), '-','_'), '.','_'), '/','_'), '[',''), ']',''), ' ','_'))
-- from information_schema.columns where table_name='sample' and table_schema='odv'
-- order by ordinal_position
-- ;
--
