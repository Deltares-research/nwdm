grant usage on schema nwdm to nwdm;
grant select ON ALL TABLES IN SCHEMA nwdm TO nwdm;
alter default privileges in schema nwdm grant select on tables to nwdm;
