-- nabewerking
-- create index if not exists ix_location_id on nwdm.measurement(location_id);
-- create index if not exists ix_measurement_geom on nwdm.measurement using gist(geom);
-- create index if not exists ix_parameter_id on nwdm.measurement(parameter_id);

-- verwijder negatieve waarden
delete
from nwdm.measurement m
using nwdm.parameter p
where p.parameter_id=m.parameter_id
and (
    (p.p35code='WATERTEMP' and m.value<-1.8)
    or
    (p.p35code in ('EPC00105','EPC00108','EPC00168','EPC00006','EPC00007','EPC00198') and m.value<0)
)
;


create index if not exists ix_unit_id on nwdm.measurement(unit_id, parameter_id);
-- unit conversie: dezelfde parameters omrekenen naar dezelfde unit
update nwdm.measurement m       -- 11m34s
set unit_id=u2.unit_id, value = m.value*cu.multiplication_factor_to_preferred_unit::decimal
from nwdm.unit u, nwdm.parameter p, import.conversie_unit cu
join nwdm.unit u2 on u2.code=cu.preferred_unit
where u.unit_id=m.unit_id
and p.parameter_id=m.parameter_id
and cu.p35code=p.p35code and cu.unit_code=u.code and cu.unit_code<>cu.preferred_unit
AND cu.preferred_unit is not null AND cu.multiplication_factor_to_preferred_unit is not null
;

-- dubbele metingen markeren:       -- 10min
update nwdm.measurement set redundant = true where measurement_id in (
    select measurement_id
    from (
        select m.measurement_id
        , don.priority
        , row_number()
        over (partition by m.date, coalesce(depth, 0), m.parameter_id, coalesce(l.geom, m.geom),m.value order by don.priority, m.measurement_id) _nr
        , m.date
        , coalesce(depth, 0)                                                                                                                                                        depth
        , m.parameter_id
        , coalesce(l.geom, m.geom)                                                                                                                                                  geom
        from nwdm.measurement m
        join nwdm.dataset ds on ds.dataset_id = m.dataset_id
        join nwdm.data_owner don on don.data_owner = ds.data_owner
        left join nwdm.location l on l.location_id = m.location_id
        where m.parameter_id is not null
    ) x
    where x._nr > 1
)
;
--------
create index if not exists ix_meas on nwdm.measurement(measurement_id,dataset_id, location_id, parameter_id, depth, date,value);
drop table if exists nwdm._temp_red_overig;
-- dubbel in emodnet in vergelijking met andere bron (op p35-niveau): voorbereiding
select m.measurement_id, l.geom, p.p35code, m.depth, m.date, m.value
into nwdm._temp_red_overig  -- 6m
from nwdm.measurement m
join nwdm.dataset ds on ds.dataset_id=m.dataset_id and ds.data_owner<>'EMODnet'
join nwdm.location l on l.location_id = m.location_id
join nwdm.parameter p on p.parameter_id = m.parameter_id;

-- dubbel in emodnet in vergelijking met andere bron (op p35-niveau): update
update nwdm.measurement set redundant=true where measurement_id in (
    select me.measurement_id
    from nwdm.measurement me
    join nwdm.dataset de on de.dataset_id=me.dataset_id and de.data_owner='EMODnet'
    join nwdm.location le on le.location_id = me.location_id
    join nwdm.parameter pe on pe.parameter_id = me.parameter_id
    join nwdm._temp_red_overig x on x.geom = le.geom and coalesce(x.depth, 0) = coalesce(me.depth, 0) and x.date = me.date and x.p35code = pe.p35code and x.value = me.value
);

-- aantallen bijwerken in location:
update nwdm.location
set number_of_observations = y.aantal
, first_year = y.first_year
, last_year = y.last_year
from (
	select location_id, count(*) aantal
	, min(extract(year from "date")) as first_year
	, max(extract(year from "date")) as last_year
	from nwdm.measurement
	where redundant=false
	group by location_id
)y
where y.location_id = nwdm.location.location_id
;

-- trema's corrigeren:
update nwdm.location set location_code = unaccent(location_code);

-- station vullen obv mapping
update nwdm.location loc set station = sta.station_name
from import.mapping_station sta
where sta.location_code=loc.location_code and sta.data_owner=loc.data_owner
;
