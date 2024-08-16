select count(*) from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ'
;

-- create backup
select m.* into nwdm._backup_measurement_vliz from nwdm.measurement m join nwdm.dataset d on d.dataset_id =m.dataset_id  where d.data_owner ='NIOZ';
select d.* into nwdm._backup_dataset_vliz from nwdm.dataset d where d.data_owner ='NIOZ';
select l.* into nwdm._backup_location_vliz from nwdm.location l  where l.data_owner ='NIOZ';