-- modelgrid_id in location bepalen:
drop index if exists nwdm.ix_location_geom;
update nwdm.location loc
set modelgrid_id = gr.modelgrid_id
from nwdm.modelgrid gr
where st_intersects(loc.geom, gr.geom);
create index if not exists ix_location_geom on nwdm.location using gist(geom);

-- modelgrid_id in measurement bijwerken obv location (dus enkel voor fixed locations): 26min
update nwdm.measurement m
set modelgrid_id = l.modelgrid_id
from nwdm.location l
where l.location_id=m.location_id
and m.location_id is not null;

-- modelgrid_id bepalen voor overige geom (ferrybox): 32min
update nwdm.measurement me
set modelgrid_id= gr.modelgrid_id
from nwdm.modelgrid gr
where st_intersects(me.geom, gr.geom)
and me.location_id is null;


drop table if exists nwdm.measurement_p35_all_modelgrid cascade;
select gr.geom, me.p35code, me.p35preflabel --5min
, me.unit_code, me.unit_preflabel
, extract(year from me.date) as year
, extract(month from me.date) as month
     , count(*) as count
     , min(value) as min_value
     , max(value) as max_value
     , avg(value) average_value
     , percentile_cont(0.25) within group(order by value) perc025_value
     , percentile_cont(0.5) within group(order by value) median_value
     , percentile_cont(0.75) within group(order by value) perc075_value
-- , array_agg(distinct data_owner),me.depth, me.datum, me.quality_code
into nwdm.measurement_p35_all_modelgrid
from nwdm.measurement_p35_all me
join nwdm.modelgrid gr on gr.modelgrid_id=me.modelgrid_id
where vertical_reference_code in ('D08', 'D99')
and (depth is null or depth between 0 and 10)
group by gr.geom, me.p35code, me.p35preflabel, me.unit_code, me.unit_preflabel
    , extract(year from me.date)
    , extract(month from me.date)
;
