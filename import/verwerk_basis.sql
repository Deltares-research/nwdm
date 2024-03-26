-- basis-tabellen (gebruikt door alle bronnen)
insert into nwdm.parameter (parameter_id, code, preflabel, altlabel, definition, parameter_origin,p35code, p35preflabel,p35altlabel,p35definition)
select 
"_recordnr" parameter_id
,conceptid as code
,preflabel
,altlabel
,definition
,'p07'::varchar parameter_origin
,null::varchar p35code
,null::varchar p35preflabel
,null::varchar p35altlabel
,null::varchar p35definition
from import."parameter"
where "_short_filename"='vocab_p07.csv'
union all
select
p35._recordnr parameter_id
,'P35_'|| p35.conceptid as code     -- voor bevraging op p35-code: gebruik kolom p35code
,p35.preflabel
,p35.altlabel
,p35.definition
,'p35'::varchar parameter_origin
,p35.conceptid p35code
,p35.preflabel p35preflabel
,p35.altlabel p35altlabel
,p35.definition p35definition
from (select * from import."parameter" where "_short_filename"='vocab_p35.csv') p35
union all
select
p01._recordnr parameter_id
,p01.conceptid as code
,p01.preflabel
,p01.altlabel
,p01.definition
,'p01'::varchar parameter_origin
,p35.conceptid p35code
,p35.preflabel p35preflabel
,p35.altlabel p35altlabel
,p35.definition p35definition
from (select * from import."parameter" where "_short_filename"='vocab_p01.csv') p01
left join (
	select mapp.p01code, par35.*
	, row_number() over (partition by mapp.p01code order by mapp.p35code) as _nr	-- om dubbele mappings te elimineren
	from import.mapping_p35_p01 mapp 
	join (select * from import."parameter" where "_short_filename"='vocab_p35.csv') par35 on par35.conceptid = mapp.p35code 
	) p35 on p35.p01code = p01.conceptid and p35._nr=1
;

insert into nwdm.unit (unit_id, code, preflabel, altlabel, definition)
select 
row_number() over (order by "_recordnr") unit_id
,conceptid as code
,preflabel
,altlabel
,definition -- select *
from import."parameter"
where "_short_filename"='vocab_p06.csv'
;

insert into nwdm.quality (quality_id, code, preflabel, altlabel, definition, use_data)
select 
row_number() over (order by "_recordnr") quality_id
,conceptid as code
,preflabel
,altlabel
,definition -- select *
,case when conceptid in ('0','1','2') then true else false end as use_data
from import."parameter"
where "_short_filename"='vocab_l20.csv'
;

insert into nwdm.vertical_reference (vertical_reference_id, code, source_code, sdn_code, preflabel, altlabel, definition)
select 
-- coalesce(par."_recordnr", row_number() over (order by mvr.referentievlak) - 100) vertical_reference_id
row_number() over (order by mvr."_recordnr")
,coalesce(par.conceptid, mvr.referentievlak) as code
, mvr.referentievlak as source_code
, par.conceptid as sdn_code
,coalesce(par.preflabel, mvr.omschrijving) preflabel
,coalesce(par.altlabel, mvr.referentievlak) altlabel
,par.definition
from import.mapping_vertical_reference mvr
full join (select * from import."parameter" where "_short_filename"='vocab_l11.csv') par on par.conceptid=mvr.sdn_code
;

-- delete from nwdm.modelgrid;
insert into nwdm.modelgrid(geom)
select geom
from import.modelgrid;
create index if not exists ix_modelgrid on nwdm.modelgrid using gist(geom);

-- vul grid obv 0.02 graden
insert into nwdm.grid(geom)
select (st_squaregrid(0.02, geom)).geom from nwdm.scope_northsea;
create index if not exists ix_grid on nwdm.grid using gist(geom);
