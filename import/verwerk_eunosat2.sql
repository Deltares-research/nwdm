
-- 2a. OBSERVATIONS EUNOSAT
delete from nwdm.measurement where dataset_id in (select dataset_id from nwdm.dataset where data_holder in ('EUNOSAT', 'INTERREG'));
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
rd._recordnr  as recordnr_dataset
, rd.cnr as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id
, (make_date(case when rd.year!='NA' then rd.year end::int, case when rd.month!='NA' then rd.month end::int, case when rd.day!='NA' then rd.day end::int)
    + make_time(case when rd.hour!='NA' then rd.hour end::int, 0,0) ) ::timestamp as "date"
, case when sample_depth != 'NA' then sample_depth end::decimal as depth
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, coalesce(m.multiplier::decimal,1.0) * (case when cvalue != 'NA' then replace(cvalue, ',','')  end::decimal) as value
, qua.quality_id as quality_id
-- select *		-- select count(*)			--822879
from (
	select
	unnest(array[1,2,3,4,5,6,7,8,9]) as cnr
	, unnest(array['din','dip','si','chlfa','sal','temp','spm','kd','secchi']) as cname
	, unnest(array["din","dip","si","chlfa","sal","temp","spm","kd","secchi"]) as cvalue
	, sub.*
	from (select * from import.eunosat) sub
) rd
left join import.mapping_eunosat_sdn m on m.p01 is not null and lower(m.csv_name)=rd.cname
join nwdm.dataset ds on ds."path"=rd."_path" and ds.short_filename=rd."_short_filename"
join nwdm.location loc on loc.location_code = 'eu_'||rd.location_name and loc.data_owner=ds.data_owner
join nwdm.quality qua on qua.code = '0'  and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = m.p06
left join nwdm.parameter par01 on par01.code = m.p01
where cvalue != 'NA'
;

-- 2b. OBSERVATIONS EUNOSAT: aanvulling schlesswig-holstein 2015-2017
set DateStyle to 'DMY';
insert into nwdm.measurement (recordnr_dataset, recordvolgnr_dataset, dataset_id, location_id, "date", "depth", vertical_reference_id, parameter_id, unit_id, value, quality_id)
select
sh._recordnr  as recordnr_dataset
, 0 as recordvolgnr_dataset
, ds.dataset_id as dataset_id
, loc.location_id
, sh.datum::date as "date"
, sh.tiefe_der_probenahme_m_chm::decimal as depth
, vr.vertical_reference_id
, par01.parameter_id as parameter_id
, un.unit_id as unit_id
, coalesce(m.multiply::decimal,1.0) * (case when sh.vorzeichen='<' then 0.5 else 1.0 end) * (replace(sh.messwert, ',','.')::decimal) as value
, qua.quality_id as quality_id
-- select count(*)			--4534
from import.eunosat_sh sh
join nwdm.dataset ds on ds."path"=sh."_path" and ds.short_filename=sh."_short_filename"
join nwdm."location" loc on left(loc.location_code,3) = 'eu_' and lower(loc.location_name) = lower(sh.messstelle) and loc.data_owner=ds.data_owner
join import.mapping_eunosat_schlesswig_holstein m on m.p01 is not null and lower(m."parameter")=lower(sh."parameter") and coalesce(lower(m.einheit), '--')=coalesce(lower(sh.einheit), '--')
join nwdm.quality qua on qua.code = '0' and qua.use_data=true
left join nwdm.vertical_reference vr on vr.code = 'D08'		-- = sea level
left join nwdm.unit un on un.code = m.p06
left join nwdm.parameter par01 on par01.code = m.p01
;
