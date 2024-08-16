create extension if not exists postgis;
create extension if not exists unaccent;
create extension if not exists pg_stat_statements;
drop schema if exists import cascade;
create schema if not exists import;

drop table if exists import.rawdata cascade;
create table import.rawdata
(
  locatie_message_id text
, locatie_code text
, locatie_naam text
, coordinatenstelsel text
, geometriepunt_x text
, geometriepunt_y text
, tijdstip text
, statuswaarde text
, bemonsteringshoogte text
, referentievlak text
, opdrachtgevendeinstantie text
, kwaliteitswaarde_code text
, aquometadata_message_id text
, parameter_wat_omschrijving text
, bemonsteringsapparaat_code text
, bemonsteringsapparaat_omschrijving text
, bemonsteringssoort_code text
, bemonsteringssoort_omschrijving text
, biotaxon_code text
, biotaxon_omschrijving text
, biotaxoncompartiment_code text
, biotaxoncompartiment_omschrijving text
, compartiment_code text
, compartiment_omschrijving text
, eenheid_code text
, eenheid_omschrijving text
, grootheid_code text
, grootheid_omschrijving text
, hoedanigheid_code text
, hoedanigheid_omschrijving text
, meetapparaat_code text
, meetapparaat_omschrijving text
, monsterbewerkingsmethode_code text
, monsterbewerkingsmethode_omschrijving text
, orgaan_code text
, orgaan_omschrijving text
, parameter_code text
, parameter_omschrijving text
, plaatsbepalingsapparaat_code text
, plaatsbepalingsapparaat_omschrijving text
, typering_code text
, typering_omschrijving text
, waardebepalingstechniek_code text
, waardebepalingstechniek_omschrijving text
, waardebepalingsmethode_code text
, waardebepalingsmethode_omschrijving text
, waardebewerkingsmethode_code text
, waardebewerkingsmethode_omschrijving text
, numeriekewaarde text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.parameter cascade;
create table import.parameter(
"conceptid" varchar
,"preflabel" varchar
,"modified" varchar
,"altlabel" varchar
,"definition" varchar
, _recordnr bigint
, _short_filename varchar
, _path varchar
);

drop table if exists import.mapping_sdn_p01 cascade;
create table import.mapping_sdn_p01
(
  id double precision
, sdn_p01_code text
, aquo_grootheid_code text
, aquo_chemischestof_code text
, aquo_biotaxon_code text
, aquo_parameter_code text
, aquo_compartiment_code text
, aquo_monstercriterium_code text
, aquo_hoedanigheid_code text
, d_begin text
, d_eind text
, d_status text
, donar_par text
, donar_hdh text
, donar_cpm text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_sdn_l201 cascade;
create table import.mapping_sdn_l201
(
  id double precision
, sdn_l201_code varchar
, aquo_kwaliteitsoordeel_code varchar
, d_begin timestamp
, d_eind text
, d_status text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_vertical_reference cascade;
create table import.mapping_vertical_reference
(
  referentievlak text
, sdn_code text
, omschrijving text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_matrix cascade;
create table import.mapping_matrix
(
  compartiment_code text
, sdn_code text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_unit cascade;
create table import.mapping_unit
(
  eenheid_code text
, grootheid_code text
, sdn_code text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_p06_p01 cascade;
create table import.mapping_p06_p01
(
  p06code text
, p01code text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_p35_p01 cascade;
create table import.mapping_p35_p01
(
  p35code text
, p01code text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_eunosat_sdn cascade;
create table import.mapping_eunosat_sdn
(
  csv_name text
, "column" text
, p01 text
, unitbefore text
, multiplier text
, unit text
, p06 text
, additional text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_eunosat_schlesswig_holstein cascade;
create table import.mapping_eunosat_schlesswig_holstein
(
  "parameter" text
, einheit text
, multiply text
, p01 text
, p06 text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.eunosat cascade;
create table import.eunosat
(
  location_name text
, latitude text
, longitude text
, msfd text
, "year" text
, "month" text
, "day" text
, "hour" text
, sample_depth text
, din text
, dip text
, si text
, chlfa text
, sal text
, "temp" text
, spm text
, kd text
, secchi text
, primprod text
, country text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;


drop table if exists import.mapping_interreg_zooplankton_fytoplankton cascade;
create table import.mapping_interreg_zooplankton_fytoplankton
(
  dataset text
, quantity_original text
, p01 text
, unit_original text
, p06 text
, opmerking text
, _recordnr int
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_stations_zooplankton cascade;
create table import.interreg_stations_zooplankton
(
  station text
, datum text
, uhrzeit text
, geogr_breite text
, geogr_lange text
, wassertiefe text
, wassertemperatur text
, ph_wert text
, salinitat_titration text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_zooplankton cascade;
create table import.interreg_zooplankton
(
  station text
, datum text
, taxon text
, abundanz text
, biovolumen text
, frischgewicht text
, trockengewicht text
, kohlenstoff text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_chlorofyl cascade;
create table import.interreg_chlorofyl
(
  station text
, datum text
, sample_time text
, proben_nr text
, chloro_gesamt text
, chloro_aktiv text
, phaeophytin text
, methode text
, _recordnr bigint
, _short_filename varchar
, _path varchar
);

drop table if exists import.interreg_phaeocystis_colonies cascade;
create table import.interreg_phaeocystis_colonies
(
  station text
, datum text
, sample_time text
, probennummer text
, probenbezeichnung text
, bemerkung_probe text
, stamm_abteilung text
, klasse text
, ordnung text
, familie text
, gattung text
, art text
, grossenklasse text
, klassenuntergrenze text
, klassenobergrenze text
, einheit_grossenklasse text
, beschreibung text
, haufigkeit text
, einheit_haufigkeit text
, methode text
, bemerkung text
, bearbeiter text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_phaeocystis_cellnumbers cascade;
create table import.interreg_phaeocystis_cellnumbers
(
  station text
, datum text
, sample_time text
, probennummer text
, probenbezeichnung text
, bemerkung_probe text
, stamm_abteilung text
, klasse text
, ordnung text
, familie text
, gattung text
, art text
, wert text
, einheit text
, methode text
, bemerkung text
, bearbeiter text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_nlwkn cascade;
create table import.mapping_nlwkn
(
  dataset text
, csv_file text
, originalparameter text
, description text
, p01 text
, use text
, unit text
, multiply text
, newunit text
, p06 text
, additionalfield text
, additionalfieldvalue text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_wq_bork cascade;
create table import.interreg_wq_bork
(
  station_id text
, datum text
, "temp" text
, ph text
, zuurstofconcentratie text
, zuurstofverzadiging text
, geleidbaarheid text
, chloride text
, saliniteit text
, salinteit_uit_chloride text
, zwevend_stof text
, gloeiverlies text
, po4 text
, ptot text
, sio2 text
, ntot text
, nh4 text
, no2 text
, no3 text
, toc text
, doc text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_wq_nney_hw cascade;
create table import.interreg_wq_nney_hw
(
  station_id text
, datum text
, po4 text
, ptot text
, tdp text
, sio2 text
, ntot text
, tdn text
, nh4 text
, no2 text
, no3 text
, zwevend_stof text
, gloeiverlies text
, phaeo text
, chlfa_active text
, totchlfa text
, chloride text
, saliniteit_sonde text
, saliniteit_titratie text
, geleidbaarheid text
, ph text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_wq_nney_w2a cascade;
create table import.interreg_wq_nney_w2a
(
  station_id text
, datum text
, tijd text
, probennummer text
, hauptprobennummer text
, nh4 text
, no3 text
, totn text
, din text
, po4 text
, totp text
, sio2 text
, doc text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_wq_nney_w2b cascade;
create table import.interreg_wq_nney_w2b
(
  station_id text
, datum text
, tijd text
, probennummer text
, hauptprobennummer text
, ph text
, watertemperatuur text
, saliniteit_titratie text
, saliniteit_psu text
, zuurstof text
, zuurstofverzadiging text
, zwevend_stof text
, gloeiverlies text
, secchi text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_koordinaten cascade;
create table import.interreg_koordinaten
(
  location_code text
, y text
, x text
, location_name text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_nney_w_1_chem cascade;
create table import.interreg_nney_w_1_chem
(
  messstelle text
, probe text
, probenahmedatum text
, bemerkung text
, temperatuur text
, ph text
, zuurstofconcentratie text
, zuurstofverzadiging text
, geleidbaarheid text
, saliniteit1 text
, saliniteit2 text
, chloride text
, nh4 text
, no2 text
, no3 text
, ntot text
, po4 text
, tp text
, sio2 text
, zwevend_stof text
, gloeiverlies text
, doc text
, toc text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_jabu_w1chem cascade;
create table import.interreg_jabu_w1chem
(
  messstelle text
, probe text
, probenahmedatum text
, bemerkung text
, temperatuur text
, ph text
, zuurstofconcentratie text
, zuurstofverzadiging text
, geleidbaarheid text
, saliniteit1 text
, saliniteit2 text
, chloride text
, zwevend_stof text
, gloeiverlies text
, nh4 text
, x1 text
, no2 text
, x2 text
, no3 text
, x3 text
, ntot text
, x4 text
, tdn text
, x5 text
, po4 text
, x6 text
, tp text
, x7 text
, tdp text
, x8 text
, sio2 text
, x9 text
, doc text
, toc text
, x10 text
, x11 text
, x12 text
, chlfa text
, totchlfa text
, phaeo text
, chlfa_aceton1 text
, chlfa_aceton2 text
, phaeo_aceton text
, secchi text
, bewolking text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_wesermundung cascade;
create table import.interreg_wesermundung
(
  messstelle text
, probe text
, probenahmedatum text
, probenahmeuhrzeit text
, bemerkung_zur_probenahme text
, probenahmeart text
, temperatuur text
, ph text
, zuurstofconcentratie text
, zuurstofverzadiging text
, geleidbaarheid text
, saliniteit1 text
, saliniteit2 text
, chloride text
, zwevend_stof text
, gloeiverlies text
, nh4 text
, no2 text
, no3 text
, ntot text
, tdn text
, po4 text
, tp text
, tdp text
, sio2 text
, doc text
, toc text
, chlfa text
, totchlfa text
, phaeo text
, chlfa_aceton1 text
, chlfa_aceton2 text
, phaeo_aceton text
, bewolking text
, secchi text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.eunosat_sh cascade;
create table import.eunosat_sh
(
  messstelle_nr text
, messstelle text
, datum text
, jahr text
, monat text
, tiefe_der_probenahme_m_chm text
, "parameter" text
, matrix text
, vorzeichen text
, messwert text
, messwert_mit_vorzeichen text
, einheit text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.interreg_silicate cascade;
create table import.interreg_silicate
(
  messstelle text
, probenahmedatum text
, sio2 text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.donar_sioene cascade;
create table import.donar_sioene
(
  loc TEXT
, datum TEXT
, tijd TEXT
, par TEXT
, bgc TEXT
, waarde TEXT
, kwc TEXT
, ehd TEXT
, dom TEXT
, wns TEXT
, hdh TEXT
, ana TEXT
, cpm TEXT
, bem TEXT
, bew TEXT
, plt_refvlak TEXT
, plt_bmh TEXT
, sgk TEXT
, org TEXT
, ivs TEXT
, btx TEXT
, btn TEXT
, gbd TEXT
, loc_type TEXT
, loc_coordsrt TEXT
, loc_x TEXT
, loc_y TEXT
, plt_x TEXT
, plt_y TEXT
, ogi TEXT
, ani TEXT
, bhi TEXT
, bmi TEXT
, vat TEXT
, sys TEXT
, typ TEXT
, tyd_begindat TEXT
, tyd_begintyd TEXT
, tyd_einddat TEXT
, tyd_eindtyd TEXT
, sta_begindat TEXT
, sta_begintyd TEXT
, sta_einddat TEXT
, sta_eindtyd TEXT
, sta_rksstatus TEXT
, extcode TEXT
, bron TEXT
, locoms TEXT
, datumtijdwaarde TEXT
, paroms TEXT
, ehdoms TEXT
, wnsoms TEXT
, hdhoms TEXT
, anaoms TEXT
, cpmoms TEXT
, bemoms TEXT
, bewoms TEXT
, sgkoms TEXT
, orgoms TEXT
, ivsoms TEXT
, btxcod TEXT
, btxoms TEXT
, btnoms TEXT
, gbdoms TEXT
, ogioms TEXT
, anioms TEXT
, bhioms TEXT
, bmioms TEXT
, vatoms TEXT
, sysoms TEXT
, typoms TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.nioz cascade;
create table import.nioz
(
  station TEXT
, bottle_id TEXT
, latitude TEXT
, longitude TEXT
, depth text
, datetime text
, temperature TEXT
, salinity TEXT
, tco2kg01_kgum TEXT
, tco2kg01_kgum_flag TEXT
, mdmap014_kgum TEXT
, mdmap014_kgum_flag TEXT
, phmassxx TEXT
, phmassxx_flag TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_nioz cascade;
CREATE TABLE import.mapping_nioz
(
  originalName TEXT
, p01 TEXT
, p06 TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.ferrybox_header;
create table import.ferrybox_header (headerline varchar, _short_filename varchar, _path varchar);

drop table if exists import.ferrybox_data;
CREATE TABLE import.ferrybox_data
(
  indexcolumn TEXT
, "time" TEXT
, lat TEXT
, lon TEXT
, "depth" TEXT
, "value" TEXT
, flag TEXT
, "parameter" TEXT
, _recordnr bigint
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.mapping_ferrybox_parameter;
create table import.mapping_ferrybox_parameter
(
  "parameter" text
, p01 text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_ferrybox_unit;
create table import.mapping_ferrybox_unit
(
  headerline text
, p06 text
, _recordnr bigint
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.socat;
CREATE TABLE import.socat
(
  cruise TEXT
, station TEXT
, "type" TEXT
, datetime TEXT
, lon TEXT
, lat TEXT
, etopo2_depth_m TEXT
, distance_to_land_km TEXT
, version TEXT
, fco2_sourceid TEXT
, qc_flag TEXT
, cruise_dataset_name TEXT
, ship_vessel_name TEXT
, principle_investigators TEXT
, data_source_doi TEXT
, data_source_reference TEXT
, data_source_metadata TEXT
, additional_metadata_documents TEXT
, sample_depth_m TEXT
, qf_sample_depth text
, water_temperature_degc TEXT
, qf_water_temperature text
, salinity_psu TEXT
, qf_salinity text
, woa2005_sea_surface_salinity_psu TEXT
, qf_woa2005_sea_surface_salinity text
, fco2_recomputed_uatm TEXT
, qv_wocebottle TEXT
, qv_odv_sample TEXT
, _recordnr BIGINT
)
;

drop table if exists import.mapping_socat;
CREATE TABLE import.mapping_socat
(
  "column_name" TEXT
, p01 TEXT
, p06 TEXT
, use TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.pml_primprod;
CREATE TABLE import.pml_primprod
(
  cruise TEXT
, station TEXT
, latitude_dec_degrees DOUBLE PRECISION
, longitude_dec_degrees DOUBLE PRECISION
, "date" TIMESTAMP
, "14C-PP(mgCm-2d-1)" DOUBLE PRECISION
, bestand TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.mapping_primaryproduction;
CREATE TABLE import.mapping_primaryproduction
(
  bestand TEXT
, "parameter" TEXT
, P01 TEXT
, P01_label TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.pml_primprod_info;
CREATE TABLE import.pml_primprod_info
(
  bestand TEXT
, "pad" TEXT
, datum_bestand TIMESTAMP
, analysis_method TEXT
, data_analysis TEXT
, data_source TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.mapping_vliz_abiotic;
CREATE TABLE import.mapping_vliz_abiotic
(
  vliz_abiotic_parameter TEXT
, p01 TEXT
, vliz_abiotic_unit TEXT
, p06 TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.vliz_station_abiotic;
CREATE TABLE import.vliz_station_abiotic
(
  Station TEXT
, "Time" TEXT
, Latitude TEXT
, Longitude TEXT
, "Ammonium_NH4(umol_N_NH4/L)" TEXT
, "Conductivity(uS/cm)" TEXT
, CPAR TEXT
, "Density(kg/m3)" TEXT
, "Fluorescence(mg/m3)" TEXT
, "Nitrate_Nitrite(umol_N_NO3_NO2/L)" TEXT
, "Nitrate_NO3(umol_N_NO3/L)" TEXT
, "Nitrite_NO2(umol_N_NO2/L)" TEXT
, "OBS(NTU)" TEXT
, PAR TEXT
, Ph TEXT
, "Phosphate_PO4(umol_P_PO4/L)" TEXT
, "Pressure(db)" TEXT
, "Salinity(PSU)" TEXT
, "Secchi_Depth(cm)" TEXT
, "Silicate_SiO4(umol_Si_SiO4/L)" TEXT
, "Sound_Velocity(m/s)" TEXT
, SPAR TEXT
, "SPM(mg/l)" TEXT
, "Temperature(degC)" TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;


drop table if exists import.ices_0676 cascade ;
CREATE TABLE import.ices_0676
(
  cruise TEXT
, station TEXT
, "type" TEXT
, "year" TEXT
, "month" TEXT
, "day" TEXT
, "hour" TEXT
, "minute" TEXT
, latitude_degrees_north TEXT
, longitude_degrees_east TEXT
, bot_depth_m TEXT
, qv_odv TEXT
, secchi_depth_m TEXT
, qv_odv_secchi_depth TEXT
, depth_m TEXT
, qv_odv_depth TEXT
, pressure_dbar TEXT
, qv_odv_pressure TEXT
, temperature_degc TEXT
, qv_odv_temperature TEXT
, practical_salinity_dmnless TEXT
, qv_odv_salinity TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.ices_0763;
CREATE TABLE import.ices_0763
(
  cruise TEXT
, station TEXT
, "type" TEXT
, "year" TEXT
, "month" TEXT
, "day" TEXT
, "hour" TEXT
, "minute" TEXT
, latitude_degrees_north TEXT
, longitude_degrees_east TEXT
, "bot._depth_m" TEXT
, qv_odv_botdepth TEXT
, depth_m TEXT
, qv_odv_depth TEXT
, temperature_degc TEXT
, qv_odv_temp TEXT
, practical_salinity_dmnless TEXT
, qv_odv_salinity TEXT
, phosphate_phosphorus_po4_p_umol_l TEXT
, qv_odv_posphate TEXT
, silicate_silicon_sio4_si_umol_l TEXT
, qv_odv_silicate TEXT
, nitrate_nitrogen_no3_n_umol_l TEXT
, qv_odv_nitrate TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.ices_1921;
CREATE TABLE import.ices_1921
(
  cruise TEXT
, station TEXT
, "type" TEXT
, "year" TEXT
, "month" TEXT
, "day" TEXT
, "hour" TEXT
, "minute" TEXT
, latitude_degrees_north TEXT
, longitude_degrees_east TEXT
, "bot._depth_m" TEXT
, qv_odv_botdepth TEXT
, secchi_depth_m_metavar_float TEXT
, qv_odv_secchidepth TEXT
, depth_m TEXT
, qv_odv_depth TEXT
, pressure_dbar TEXT
, qv_odv_pressure TEXT
, temperature_degc TEXT
, qv_odv_temperature TEXT
, practical_salinity_dmnless TEXT
, qv_odv_salinity TEXT
, dissolved_oxygen_ml_l TEXT
, qv_odv_disoxyg_ml_l TEXT
, dissolved_oxygen_ml_kg TEXT
, qv_odv_disoxy_ml_kg TEXT
, phosphate_phosphorus_po4_p_umol_l TEXT
, qv_odv_phosphate TEXT
, silicate_silicon_sio4_si_umol_l TEXT
, qv_odv_silicate TEXT
, nitrate_nitrogen_no3_n_umol_l TEXT
, qv_odv_nitrate TEXT
, hydrogen_ion_concentration_ph_ph TEXT
, qv_odv_hydrogen TEXT
, chlorophyll_a_ug_l TEXT
, qv_odv_chlorophyll TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.ices_2086;
CREATE TABLE import.ices_2086
(
  cruise TEXT
, station TEXT
, "type" TEXT
, "year" TEXT
, "month" TEXT
, "day" TEXT
, "hour" TEXT
, "minute" TEXT
, latitude_degrees_north TEXT
, longitude_degrees_east TEXT
, bot_depth_m TEXT
, qv_odv_botdepth TEXT
, depth_m TEXT
, qv_odv_depth TEXT
, temperature_degc TEXT
, qv_odv_temperature TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.ices_5227;
CREATE TABLE import.ices_5227
(
  cruise TEXT
, station TEXT
, "type" TEXT
, "year" TEXT
, "month" TEXT
, "day" TEXT
, "hour" TEXT
, "minute" TEXT
, latitude_degrees_north TEXT
, longitude_degrees_east TEXT
, "bot._depth_m" TEXT
, qv_odv_botdepth TEXT
, secchi_depth_m_metavar_float TEXT
, qv_odv_secchidepth TEXT
, depth_m TEXT
, qv_odv_depth TEXT
, pressure_dbar TEXT
, qv_odv_pressure TEXT
, temperature_degc TEXT
, qv_odv_temperature TEXT
, practical_salinity_dmnless TEXT
, qv_odv_salinity TEXT
, dissolved_oxygen_ml_l TEXT
, qv_odv_disoxyg_ml_l TEXT
, dissolved_oxygen_ml_kg TEXT
, qv_odv_disoxyg_ml_kg TEXT
, phosphate_phosphorus_po4_p_umol_l TEXT
, qv_odv_phosphate_umol_l TEXT
, phosphate_phosphorus_po4_p_umol_kg TEXT
, qv_odv_phosphate_umol_kg TEXT
, total_phosphorus_p_umol_l TEXT
, qv_odv_phosphorus TEXT
, silicate_silicon_sio4_si_umol_l TEXT
, qv_odv_silicate_umol_l TEXT
, silicate_silicon_sio4_si_umol_kg TEXT
, qv_odv_silicate_umol_kg TEXT
, nitrate_nitrogen_no3_n_umol_l TEXT
, qv_odv_nitrate_umol_l TEXT
, nitrate_nitrogen_no3_n_umol_kg TEXT
, qv_odv_nitrate_umol_kg TEXT
, nitrite_nitrogen_no2_n_umol_l TEXT
, qv_odv_nitrite_umol_l TEXT
, nitrite_nitrogen_no2_n_umol_kg TEXT
, qv_odv_nitrite_umol_kg TEXT
, ammonium_nitrogen_nh4_n_umol_l TEXT
, qv_odv_ammon_umol_l TEXT
, ammonium_nitrogen_nh4_n_umol_kg TEXT
, qv_odv_ammon_umol_kg TEXT
, total_nitrogen_n_umol_l TEXT
, qv_odv_total_nitrogen TEXT
, hydrogen_sulphide_h2s_s_umol_l TEXT
, qv_odv_hydrogen_sulphide TEXT
, hydrogen_ion_concentration_ph_ph TEXT
, qv_odv_hydrogen_ion_conc TEXT
, alkalinity_meq_l TEXT
, qv_odv_alkalinity_l TEXT
, alkalinity_meq_kg TEXT
, qv_odv_alkalinity_kg TEXT
, chlorophyll_a_ug_l TEXT
, qv_odv_chlorophyll_a_ug_l TEXT
, chlorophyll_a_ug_kg TEXT
, qv_odv_chlorophyll_a_ug_kg TEXT
, _recordnr BIGINT
, _short_filename varchar
, _path varchar
)
;

drop table if exists import.mapping_station;
CREATE TABLE import.mapping_station
(
  data_owner TEXT
, location_code TEXT
, location_name TEXT
, xy TEXT
, dups TEXT
, modelnames TEXT
, station_name TEXT
, descriptive_name TEXT
, what_to_do TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.mapping_ices;
CREATE TABLE import.mapping_ices
(
  "table_name" TEXT
, bestandsnaam TEXT
, "column_name" TEXT
, ordinal_position DOUBLE PRECISION
, p01 TEXT
, p35 TEXT
, p06 TEXT
, factor TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.conversie_unit;
CREATE TABLE import.conversie_unit
(
  p35code TEXT
, unit_code TEXT
, preferred_unit TEXT
, multiplication_factor_to_preferred_unit TEXT
, opmerking TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.mapping_macovei;
CREATE TABLE import.mapping_macovei
(
  "column_name" TEXT
, p01 TEXT
, p35 TEXT
, p06 TEXT
, factor TEXT
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;

drop table if exists import.macovei;
CREATE TABLE import.macovei
(
  date_time TEXT
, latitude TEXT
, longitude TEXT
, pco2water_sst_wet_uatm TEXT
, temp_c TEXT
, sal text
, _recordnr BIGINT
, _short_filename VARCHAR
, _path VARCHAR
)
;