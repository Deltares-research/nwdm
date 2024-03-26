-- grid_id in location bepalen:
update nwdm.location loc
set grid_id = gr.grid_id
from nwdm.grid gr
where st_intersects(loc.geom, gr.geom)
;

-- grid_id in measurement bijwerken obv location (dus enkel voor fixed locations):
update nwdm.measurement m -- 46 min
set grid_id = l.grid_id
from nwdm.location l
where l.location_id=m.location_id
and m.location_id is not null
;

-- grid_id bepalen voor overige geom (ferrybox): 2 h 28 m
update nwdm.measurement me
set grid_id= gr.grid_id
from nwdm.grid gr
where st_intersects(me.geom, gr.geom)
and me.location_id is null;


drop table if exists nwdm.measurement_p35_all_grid cascade;
select gr.geom, me.p35code, me.p35preflabel -- 20 m 42 s min
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
into nwdm.measurement_p35_all_grid
from nwdm.measurement_p35_all me
join nwdm.grid gr on gr.grid_id=me.grid_id
where vertical_reference_code in ('D08', 'D99')
and (depth is null or depth between 0 and 10)
group by gr.geom, me.p35code, me.p35preflabel, me.unit_code, me.unit_preflabel
    , extract(year from me.date)
    , extract(month from me.date)
;
