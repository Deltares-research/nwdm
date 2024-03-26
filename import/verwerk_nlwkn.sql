-- verwerking NLWKN/Interreg-data

--0. data_owners
insert into nwdm.data_owner (data_owner,priority) values ('NLWKN',5) on conflict do nothing ;


--3ab.dataset NLWKN (interreg) (zooplankton+phytoplankton)
-- delete from nwdm.dataset where dataset_id > 200000 ;
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner='NLWKN');
delete from nwdm.dataset where data_owner='NLWKN';
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 200000 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'INTERREG' data_holder
, 'NLWKN' data_owner
, 'p:\11204882-002-interreg-wadden-sea\validatie_data\csv_data\' link_to_data
, 'p:\11204882-002-interreg-wadden-sea\validatie_data\csv_data\'::varchar link_to_metadata	-- select *
from (
	select _short_filename, _path from import.interreg_zooplankton
	union all
	select _short_filename, _path from import.interreg_chlorofyl
	union all
	select _short_filename, _path from import.interreg_phaeocystis_colonies
	union all
	select _short_filename, _path from import.interreg_phaeocystis_cellnumbers
) x
group by "_short_filename", "_path";
--3c.dataset interreg (wq)
-- delete from nwdm.dataset where dataset_id > 200100;
insert into nwdm.dataset(dataset_id, dataset_name, short_filename, "path", file, number_of_records, data_holder, data_owner, link_to_data, link_to_metadata)
select 200100 + row_number() over (order by _path,"_short_filename") dataset_id
, replace(_short_filename, '.csv', '') as dataset_name
,"_short_filename" short_filename
, "_path" "path"
, _path ||'\'||"_short_filename" as file
, count(*) number_of_records
, 'INTERREG' data_holder
, 'NLWKN' data_owner
, (select additionalfieldvalue from import.mapping_nlwkn where additionalfield='link_to_data') link_to_data
, (select additionalfieldvalue from import.mapping_nlwkn where additionalfield='link_to_metadata') link_to_metadata
-- select *
from (
	select _short_filename, _path from import.interreg_wq_bork
	union all
	select _short_filename, _path from import.interreg_wq_nney_hw
	union all
	select _short_filename, _path from import.interreg_wq_nney_w2a
	union all
	select _short_filename, _path from import.interreg_wq_nney_w2b
	union all
	select _short_filename, _path from import.interreg_nney_w_1_chem
	union all
	select _short_filename, _path from import.interreg_jabu_w1chem
	union all
	select _short_filename, _path from import.interreg_wesermundung
	union all
	select _short_filename, _path from import.interreg_silicate
) x
group by "_short_filename", "_path";


--3.interreg locations
delete from nwdm.location where data_owner='NLWKN';
insert into nwdm.location(location_code, location_name,x,y,epsg,geom,data_owner)
select *
from (
    select
     rd.location_code as location_code
    , rd.location_name as location_name
    , (rd.x)::decimal as x
    , (rd.y)::decimal as y
    , 4326::int as epsg
    , st_setsrid(st_makepoint(x,y), 4326::int) as geom
    , 'NLWKN' data_owner
    from (	select location_code, location_name
    --		, station ||'_'|| coalesce(to_char(datum::timestamp, 'YYYYMMDD'), '') as location_code
            , x1 + (x2/60) + (x3/3600) ::decimal as x
            , y1 + (y2/60) + (y3/3600) ::decimal as y
    --		, row_number() over (partition by station order by datum) as _nr
            -- select *
            from (select *      -- graden-teken ('°'): chr(176)
                , split_part(y,chr(176),1)::decimal y1
                , ltrim(replace(split_part((split_part(y,chr(176),2)),'''',1), ',','.'))::decimal y2
                , ltrim(replace(split_part((split_part(y,chr(176),2)),'''',2), ',','.'))::decimal y3
                , split_part(x,chr(176),1)::decimal x1
                , ltrim(replace(split_part((split_part(x,chr(176),2)),'''',1), ',','.'))::decimal x2
                , ltrim(replace(split_part((split_part(x,chr(176),2)),'''',2), ',','.'))::decimal x3
                from import.interreg_koordinaten
                ) s
    ) rd
) g where st_contains((select geom from nwdm.scope_northsea),g.geom);
;



--3a. OBSERVATIONS INTERREG zooplankton
set DateStyle to 'DMY';
-- delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_owner='NLWKN');
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id
, rd.datum::date as "date"
-- , null::time as begin_time
--, make_date(case when rd.year!='NA' then rd.year end::int, case when rd.month!='NA' then rd.month end::int, case when rd.day!='NA' then rd.day end::int) as "date"
-- , null as end_time
--, loci.wassertiefe as depth
, null::int as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, cvalue::decimal as value
, qua.quality_id as quality_id
-- select *		-- select count(*)			--1340
from (
	select
	unnest(array[1,2,3,4,5]) as cnr
	, unnest(array['abundanz','biovolumen','frischgewicht','trockengewicht','kohlenstoff']) as cname
	, replace(unnest(array["abundanz","biovolumen","frischgewicht","trockengewicht","kohlenstoff"]), ',','') as cvalue -- komma=duizendscheidingsteken
	, sub.*
--	, station ||'_'|| coalesce(to_char(datum::timestamp, 'YYYYMMDD'),'') as location_code
	from (select * from import.interreg_zooplankton) sub
) rd
left join import.mapping_interreg_zooplankton_fytoplankton m on m.p01 is not null and m.dataset='zooplankton_summe' and lower(m.quantity_original)=cname
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on lower(loc.location_code) = lower(rd.station) and loc.data_owner='NLWKN'
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'UKN'		--rd.referentievlak
join nwdm.unit un on un.code = m.p06
/*left*/ join nwdm.parameter par01 on par01.code = m.p01
;
--3b observations INTERREG phytoplankton chlorophyll
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id
, rd.datum::date as "date"
-- , null::time as begin_time
-- , null as end_time
--, loci.wassertiefe as depth
, null::int as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, cvalue::decimal as value
, qua.quality_id as quality_id
-- select *		-- select count(*)			--4998
from (
	select
	unnest(array[1,2,3]) as cnr
	, unnest(array['chloro_gesamt', 'chloro_aktiv','phaeophytin']) as cname
	, replace(unnest(array["chloro_gesamt", "chloro_aktiv","phaeophytin"]), ',','') as cvalue -- komma=duizendscheidingsteken
	, sub.*
--	, station ||'_'|| coalesce(to_char(datum::timestamp, 'YYYYMMDD'),'') as location_code
	from (select * from import.interreg_chlorofyl) sub
) rd
left join import.mapping_interreg_zooplankton_fytoplankton m on m.p01 is not null and m.dataset='phytoplankton' and lower(m.quantity_original)=cname
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on lower(loc.location_code) = lower(rd.station) and loc.data_owner='NLWKN'
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'UKN'		--rd.referentievlak
join nwdm.unit un on un.code = m.p06
join nwdm.parameter par01 on par01.code = m.p01
;

--3c observations INTERREG wq
set DateStyle to 'DMY';
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id
, rd.datum::date as "date"
-- , null::time as begin_time
-- , null as end_time
--, loci.wassertiefe as depth
, null::int as depth
, vr.vertical_reference_id
-- , mat.matrix_id as matrix_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
--, replace(replace(cvalue, 'not measured',null), '<','')::decimal as value
, case when cvalue = 'not measured' or cvalue = '#VALUE!' then null else replace(replace(cvalue,',','.'), '<','') end ::decimal * coalesce(m.multiply::decimal,1.0) as value
, qua.quality_id as quality_id
-- select *		-- select count(*)
from (
	select
	unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]) as cnr
	, unnest(array['temp','ph','zuurstofconcentratie','zuurstofverzadiging','geleidbaarheid','chloride','saliniteit','salinteit_uit_chloride','zwevend_stof','gloeiverlies','po4','ptot','sio2','ntot','nh4','no2','no3','toc','doc']) as cname
	, replace(unnest(array["temp","ph","zuurstofconcentratie","zuurstofverzadiging","geleidbaarheid","chloride","saliniteit","salinteit_uit_chloride","zwevend_stof","gloeiverlies","po4","ptot","sio2","ntot","nh4","no2","no3","toc","doc"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, sub.station_id, sub.datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_wq_bork) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]) as cnr
	, unnest(array['po4','ptot','tdp','sio2','ntot','tdn','nh4','no2','no3','zwevend_stof','gloeiverlies','phaeo','chlfa_active','totchlfa','chloride','saliniteit_sonde','saliniteit_titratie','geleidbaarheid','ph']) as cname
	, replace(unnest(array["po4","ptot","tdp","sio2","ntot","tdn","nh4","no2","no3","zwevend_stof","gloeiverlies","phaeo","chlfa_active","totchlfa","chloride","saliniteit_sonde","saliniteit_titratie","geleidbaarheid","ph"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, sub.station_id, sub.datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_wq_nney_hw) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8]) as cnr
	, unnest(array['nh4','no3','totn','din','po4','totp','sio2','doc']) as cname
	, replace(unnest(array["nh4","no3","totn","din","po4","totp","sio2","doc"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, sub.station_id, sub.datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_wq_nney_w2a) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8,9]) as cnr
	, unnest(array['ph','watertemperatuur','saliniteit_titratie','saliniteit_psu','zuurstof','zuurstofverzadiging','zwevend_stof','gloeiverlies','secchi']) as cname
	, replace(unnest(array["ph","watertemperatuur","saliniteit_titratie","saliniteit_psu","zuurstof","zuurstofverzadiging","zwevend_stof","gloeiverlies","secchi"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, sub.station_id, sub.datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_wq_nney_w2b) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]) as cnr
	, unnest(array['temperatuur','ph','zuurstofconcentratie','zuurstofverzadiging','geleidbaarheid','saliniteit1','saliniteit2','chloride','nh4','no2','no3','ntot','po4','tp','sio2','zwevend_stof','gloeiverlies','doc','toc']) as cname
	, replace(unnest(array["temperatuur","ph","zuurstofconcentratie","zuurstofverzadiging","geleidbaarheid","saliniteit1","saliniteit2","chloride","nh4","no2","no3","ntot","po4","tp","sio2","zwevend_stof","gloeiverlies","doc","toc"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, substring(sub.messstelle from '[^ ]+'::text) station_id, sub.probenahmedatum datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_nney_w_1_chem) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27]) as cnr
	, unnest(array['temperatuur','ph','zuurstofconcentratie','zuurstofverzadiging','geleidbaarheid','saliniteit1','saliniteit2','chloride','zwevend_stof','gloeiverlies','nh4','no2','no3','ntot','tdn','po4','tp','tdp','sio2','doc','toc','chlfa','totchlfa','phaeo','chlfa_aceton1','chlfa_aceton2','phaeo_aceton','secchi','bewolking']) as cname
	, replace(unnest(array["temperatuur","ph","zuurstofconcentratie","zuurstofverzadiging","geleidbaarheid","saliniteit1","saliniteit2","chloride","zwevend_stof","gloeiverlies","nh4","no2","no3","ntot","tdn","po4","tp","tdp","sio2","doc","toc","chlfa","totchlfa","phaeo","chlfa_aceton1","chlfa_aceton1","phaeo_aceton","secchi","bewolking"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, substring(sub.messstelle from '[^ ]+'::text) station_id, sub.probenahmedatum datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_jabu_w1chem) sub
	union all
	select
	unnest(array[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26]) as cnr
	, unnest(array['temperatuur','ph','zuurstofconcentratie','zuurstofverzadiging','geleidbaarheid','saliniteit1','saliniteit2','chloride','zwevend_stof','gloeiverlies','nh4','no2','no3','ntot','tdn','po4','tp','tdp','sio2','doc','toc','chlfa','totchlfa','phaeo','chlfa_aceton1','chlfa_aceton2','phaeo_aceton','bewolking']) as cname
	, replace(unnest(array["temperatuur","ph","zuurstofconcentratie","zuurstofverzadiging","geleidbaarheid","saliniteit1","saliniteit2","chloride","zwevend_stof","gloeiverlies","nh4","no2","no3","ntot","tdn","po4","tp","tdp","sio2","doc","toc","chlfa","totchlfa","phaeo","chlfa_aceton1","chlfa_aceton2","phaeo_aceton","bewolking"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, substring(sub.messstelle from '[^ ]+'::text) station_id, sub.probenahmedatum datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_wesermundung) sub
	union all
	select
	unnest(array[1]) as cnr
	, unnest(array['sio2']) as cname
	, replace(unnest(array["sio2"]), ',','.') as cvalue -- komma=duizendscheidingsteken
	, substring(sub.messstelle from '[^ ]+'::text) station_id, sub.probenahmedatum datum, sub._recordnr, sub._short_filename, sub._path
	from (select * from import.interreg_silicate) sub
) rd
join import.mapping_nlwkn m on m.p01 is not null and m.use='x' and lower(m.description)=cname and m.csv_file=rd._short_filename
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on lower(loc.location_code) = lower(rd.station_id) and loc.data_owner='NLWKN'
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'UKN'		--rd.referentievlak
join nwdm.unit un on un.code = m.p06
join nwdm.parameter par01 on par01.code = m.p01
;
