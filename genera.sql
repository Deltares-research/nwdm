select * from nwdm.dataset as d where d.data_owner  = 'SOCAT';

select MIN(datetime) as MIN_DATE from import.socat s;

select MAX(datetime) as MIN_DATE from import.socat s;

select * from import.socat s;

select * from import.socat s where s.cruise = '069920190503';

select * from nwdm.dataset as d where d.data_owner  = 'PML';

select * from nwdm.dataset as d where d.data_owner  = 'Rijkswaterstaat';

select * from nwdm.dataset as d where d.data_owner  = 'Macovei';
select * from nwdm.dataset as d where d.data_owner  = 'HZG';
select * from nwdm.dataset as d where d.data_owner  = 'EMODnet';
select * from nwdm.dataset as d where d.data_owner  = 'SOCAT';

select * from nwdm.dataset as d where d.data_owner  = 'Schlesswig-Holstein';




select MIN(datetime) as MIN_DATE from import.socat s;
select MAX(time) as MAX_DATE from import.ferrybox_data fd;
select * from import.socat s;
select * from import.socat s where s.cruise = '069920190503';

---------------------------------------------------------------

-- ** Goal is to retrieve the sea level and bottom temp. p35code = WATERTEMP
-- ** view: measurement_p35_all


-- vertical_reference_code D08, vertical_reference_preflabel = se level
--- GOERE6, HAMMOT, LODSGT
SELECT s.location_code, s.date, s.p35code, s."depth", s.value, s.vertical_reference_preflabel, s.vertical_reference_code, s.unit_preflabel, s.quality_code, s.station, s.geom, s.data_owner
FROM (
    SELECT m.location_code, m.date, m.p35code, m."depth", m.value, m.vertical_reference_preflabel, m.vertical_reference_code, m.unit_preflabel, m.quality_code, m.station, m.geom, m.data_owner
    FROM nwdm.measurement_p35_all AS m
    WHERE m.p35code = 'WATERTEMP'
) AS s
WHERE s.vertical_reference_preflabel = 'sea floor' and s.location_code = 'LODSGT'
GROUP by s.date, s.location_code, s.p35code, s."depth", s.value, s.vertical_reference_preflabel, s.vertical_reference_code, s.unit_preflabel, s.quality_code, s.station, s.geom, s.data_owner
ORDER by s.date ASC



select MIN(s.depth)
FROM (
    SELECT m.location_code, m.date, m.p35code, m."depth", m.value, m.vertical_reference_preflabel, m.vertical_reference_code, m.unit_preflabel, m.quality_code, m.station, m.geom
    FROM nwdm.measurement_p35_all AS m
    WHERE m.p35code = 'WATERTEMP'
) AS s
WHERE s.vertical_reference_preflabel = 'sea floor'






select distinct m.vertical_reference_preflabel from nwdm.measurement_p35_all as m;

--- count number of rows for checking how many were before; 1617
select count(*) from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ';
--- 849
select count(*) from import.nioz n;

--- create a backup of measurent, dataset, location where data owner is NIOZ
select m.* into nwdm._backup_measurement_nioz from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ';
select d.* into nwdm._backup_dataset_nioz from nwdm.dataset d where d.data_owner ='NIOZ';
select l.* into nwdm._backup_location_nioz from nwdm.location l  where l.data_owner ='NIOZ';



select * from nwdm.dataset as d where d.data_owner  = 'PML';


select MIN(time_coverage_start) as MIN_DATE from import.socat_v2023_fulldata s;
select MAX(time_coverage_start) as MAX_DATE from import.socat_v2023_fulldata s;

select MIN(datetime) as MIN_DATE from import.socat_v2023_fulldata s;
select MAX(datetime) as MAX_DATE from import.socat;


select * from import.socat s;
select * from import.socat s where s.cruise = '069920190503';
