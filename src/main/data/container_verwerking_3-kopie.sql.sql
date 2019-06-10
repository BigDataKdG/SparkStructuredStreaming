-----------------------------------Container tabel-----------------------------------

---Tabel inladen 

--drop table container cascade;
create table container(
"Datum & tijdstip containeractiviteit" varchar(250),
"Container nr" varchar(250),
"Datum containermelding" varchar(250),
"Tijdstip" varchar(250),
"Containermelding ID" varchar(250),
"Containermelding naam" varchar(250),
"Containermelding omschrijving" varchar(250),
"Containermelding categorie code" varchar(250),
"Containermelding categorie omschrijving" varchar(250),
"Container nummer" varchar(250),
"Container locatie" varchar(250),
"Container wijk" varchar(250),
"Container straatcode" varchar(250),
"Container straat" varchar(250),
"Container postcode" varchar(250),
"Container district" varchar(250),
"Container verplicht?" varchar(250),
"Container PO nummer" varchar(250),
"Container ledigingsfrequentie" varchar(250),
"Container ledigingsdag" varchar(250),
"Container route" varchar(250),
"Container afvaltype" varchar(250),
"Container type" varchar(250));

ALTER TABLE container RENAME "Datum & tijdstip containeractiviteit" to datum_tijdstip_containeractiviteit;
ALTER TABLE container RENAME "Container nr" to container_nr;
ALTER TABLE container RENAME "Datum containermelding" to datum_containermelding;
ALTER TABLE container RENAME "Tijdstip" to tijdstip;
ALTER TABLE container RENAME "Containermelding ID" to containermelding_id;
ALTER TABLE container RENAME "Containermelding naam" to containermelding_naam;
ALTER TABLE container RENAME "Containermelding omschrijving" to containermelding_omschrijving;
ALTER TABLE container RENAME "Containermelding categorie code" to containermelding_categorie_code;
ALTER TABLE container RENAME "Containermelding categorie omschrijving" to containermelding_categorie_omschrijving;
ALTER TABLE container RENAME "Container nummer" to container_nummer;
ALTER TABLE container RENAME "Container locatie" to container_locatie;
ALTER TABLE container RENAME "Container wijk" to container_wijk;
ALTER TABLE container RENAME "Container straatcode" to container_straatcode;
ALTER TABLE container RENAME "Container straat" to container_straat;
ALTER TABLE container RENAME "Container postcode" to container_postcode;
ALTER TABLE container RENAME "Container district" to container_district;
ALTER TABLE container RENAME "Container verplicht?" to container_verplicht;
ALTER TABLE container RENAME "Container PO nummer" to container_po_nummer;
ALTER TABLE container RENAME "Container ledigingsfrequentie" to container_ledigingsfrequentie;
ALTER TABLE container RENAME "Container ledigingsdag" to container_ledigingsdag;
ALTER TABLE container RENAME "Container route" to container_route;
ALTER TABLE container RENAME "Container afvaltype" to container_afvaltype;
ALTER TABLE container RENAME "Container type" to container_type;
	 
COPY container
FROM 'C:\temp\split\Nieuw rapport T11_06_51_chunk1.csv' DELIMITER '	' CSV HEADER ENCODING 'UTF8'; 						 
COPY container
FROM 'C:\temp\split\Nieuw rapport T11_06_51_chunk2.csv' DELIMITER '	' CSV HEADER ENCODING 'UTF8'; 
COPY container
FROM 'C:\temp\split\Nieuw rapport T11_06_51_chunk3.csv' DELIMITER '	' CSV HEADER ENCODING 'UTF8'; 
COPY container
FROM 'C:\temp\split\Nieuw rapport T11_06_51_chunk4.csv' DELIMITER '	' CSV HEADER ENCODING 'UTF8'; 
COPY container
FROM 'C:\temp\split\Nieuw rapport T11_06_51_chunk5.csv' DELIMITER '	' CSV HEADER ENCODING 'UTF8'; 

delete from container where container_nr = 'Container nr'; 

						 
---drop view container_selecti cascade;				 
---create view container_selecti as
--select * from container where container_nr = '222';
				
--- select * from container_selecti;
						 
---drop view alle_ledigingen cascade;						 
--- alle ledigingen, met timestamp. In de eerste view vragen we alle ledigingen op met code 11 of 77 (effectieve tipping signaal/emptying)			 
create or replace view alle_ledigingen as 
select TO_TIMESTAMP(datum_tijdstip_containeractiviteit, 'YYYY/MM/DD HH24:MI') timestamp_melding, container_nr,containermelding_categorie_code, containermelding_naam, containermelding_id
	 from container
	 where containermelding_id = '11' or containermelding_id = '77'
	 order by container_nr, 1;

-- alle ledigingen met volgende timestamps. In deze view transformeren we de vorige ledigingen tabel zodat er intervallen gevormd worden waartussen ledingingen hebben plaatsgevonden. 
create or replace view ledigingen_interval as 						 
SELECT timestamp_melding,
	lead(timestamp_melding) OVER (partition by container_nr ORDER BY timestamp_melding ASC) as timestamp_volgende_lediging, 
	container_nr, containermelding_categorie_code
FROM alle_ledigingen
ORDER BY container_nr, timestamp_melding;

--drop view container_met_volgende_ledigingen cascade;
-- Dus elke rij hier is een storting met een kolom wanneer de laatste lediging was, dit is de basis tabel waarop tellingen kunnen gebeuren.
-- Deze werd gemaakt door de op basis van de datum van de storting, om dit dan te koppelen aan de juiste interval van de lediging.
create or replace view container_met_volgende_ledigingen as 					 
select c.container_nr,TO_TIMESTAMP(c.datum_tijdstip_containeractiviteit, 'YYYY/MM/DD HH24:MI') as timestamp_melding_storting, to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') dag_storting,
c.containermelding_categorie_code, l.timestamp_melding,l.timestamp_volgende_lediging,containermelding_id
from container as c
left join ledigingen_interval as l
on l.container_nr = c.container_nr and (l.timestamp_melding, l.timestamp_volgende_lediging) OVERLAPS (TO_TIMESTAMP(c.datum_tijdstip_containeractiviteit, 'YYYY/MM/DD HH24:MI'), TO_TIMESTAMP(c.datum_tijdstip_containeractiviteit, 'YYYY/MM/DD HH24:MI'))
where c.containermelding_categorie_code = 'STRT'
order by 1, 2 ;
 
-- Dan is het het een kwestie van een telling te doen in deze tabel. Dit gebeurd door het aantal rijen te tellen met dezelfde dag_storting (elke rij is een storting).
-- Dit gebeurd voor kleine en grote stortingen
-- eind resultaat is dus per dag, het aantal stortingen
create or replace view container_met_volgende_ledigingen_count_big as
select container_nr, dag_storting, containermelding_categorie_code, timestamp_melding, count(*) aantal_big
from container_met_volgende_ledigingen
where containermelding_id = '21' 
group by container_nr, dag_storting, containermelding_categorie_code, timestamp_melding
order by 2;

create or replace view container_met_volgende_ledigingen_count_small as
select container_nr, dag_storting, containermelding_categorie_code, timestamp_melding, count(*) aantal_small
from container_met_volgende_ledigingen
where containermelding_id = '20'
group by container_nr, dag_storting, containermelding_categorie_code, timestamp_melding
order by 2;

---drop view  container_met_volgende_ledigingen_count_sum cascade;
-- Het laatste dat er dan nog moet gebeuren om de effectieve telling te krijgen sinds de laaste lediging, is een sommatie van het aantal ( voor elke container over de laatste lediging )
create or replace view container_met_volgende_ledigingen_count_sum as
select j.container_nr, j.dag_storting, b.containermelding_categorie_code, b.timestamp_melding,
sum(aantal_big) over (partition by b.container_nr,b.timestamp_melding order by b.dag_storting) as telling_big_sinds_lediging,
sum(aantal_small) over (partition by s.container_nr,s.timestamp_melding order by s.dag_storting) as telling_small_sinds_lediging
from (select container_nr, dag_storting from container_met_volgende_ledigingen group by container_nr, dag_storting  order by 2,1) as j 
left join container_met_volgende_ledigingen_count_small s on j.container_nr =s.container_nr and j.dag_storting=s.dag_storting
left join container_met_volgende_ledigingen_count_big b  on b.container_nr = j.container_nr and b.dag_storting = j.dag_storting 
order by b.container_nr, b.dag_storting;

--- NULL moet 0 worden (in de volgende queries gebreuren er berekeningen)
create or replace view container_met_volgende_ledigigen_count_sum_null as 
select container_nr, dag_storting, containermelding_categorie_code, timestamp_melding,
CASE when telling_big_sinds_lediging is null then 0 else telling_big_sinds_lediging end telling_big_sinds_lediging,
CASE when telling_small_sinds_lediging is null then 0 else telling_small_sinds_lediging end telling_small_sinds_lediging
from container_met_volgende_ledigingen_count_sum; 

---drop view container_count_per_dag_big;
---drop view container_count_per_dag_small;
-- Dan maken we een tabel met het aantal stortingen per dag (voor klein en groot)
create or replace view container_count_per_dag_big as
select container_nr, to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') dag, count(*) telling_big
from container c
where containermelding_categorie_code = 'STRT' and containermelding_id = '21' 
group by 1,2
order by 1;

create or replace view container_count_per_dag_small as
select container_nr, to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') dag, count(*) telling_small
from container c
where containermelding_categorie_code = 'STRT' and containermelding_id = '20' 
group by 1,2
order by 1
;
-- samenvoegen
-- drop view container_count_per_dag cascade;
create or replace view container_count_per_dag as 
select j.container_nr, j.dag_storting, s.telling_small, telling_big
from (select container_nr, to_date(SPLIT_PART(datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') dag_storting from container group by container_nr, dag_storting  order by 2,1) as j 
left join container_count_per_dag_small s on s.container_nr=j.container_nr and s.dag=j.dag_storting
left join container_count_per_dag_big b on j.container_nr=b.container_nr and j.dag_storting=b.dag;

-- NULL naar 0
create or replace view container_count_per_dag_null as 
select container_nr, dag_storting, 
CASE when telling_big is null then 0 else telling_big end telling_big,
CASE when telling_small is null then 0 else telling_small end telling_small
from container_count_per_dag; 


-- Dan de effectieve query. Hier komen een aantal zaken samen. (1) de afhankelijke: of er de volgende dag een lediging is. (2) het aantal stortingen sinds de laatste lediging (in volume uitgedrukt, dus het aantal kleine/grote stortingen maal het volume)
-- (3) het volume die dag gestort. In rij is 1 dag, voor 1 fractie. 
---drop view ml_input;

create or replace view ml_input as 
select to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') datum, c.container_nr, c.container_afvaltype,
	CASE when to_date(SPLIT_PART(datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD')+1 || c.container_nr||c.container_afvaltype in 
																																		(select to_date(SPLIT_PART(datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD')|| container_nr || container_afvaltype identifier
																																		from container 
																																		where containermelding_id = '11' or containermelding_id = '77'
																																		group by to_date(SPLIT_PART(datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD'), container_nr, container_afvaltype)
	then 1 else 0 end lediging_24h_later,  (t.telling_big_sinds_lediging*60) + (telling_small_sinds_lediging*30) volume_sinds_lediging , (d1.telling_big*60)+(d1.telling_small*30) volume_dag
	from container as c
	left join container_met_volgende_ledigigen_count_sum_null  t on c.container_nr = t.container_nr and t.dag_storting = to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD')
	left join container_count_per_dag_null  d1 on t.container_nr = d1.container_nr and t.dag_storting = d1.dag_storting
	group by to_date(SPLIT_PART(c.datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD'), c.container_nr, c.container_afvaltype,  (t.telling_big_sinds_lediging*60) + (telling_small_sinds_lediging*30),  (d1.telling_big*60)+(d1.telling_small*30)
	order by container_nr ASC, 1, 5 DESC;

COPY (select * from ml_input) TO 'C:\temp\ml_input_3.csv' DELIMITER ',' CSV HEADER;

---Voor verdere cleaning lezen we de tabel terug in (maximum memory bereikt)

drop table ml_input_zonder cascade;

create table ml_input_zonder (
datum date,
container_nr varchar(250),
container_afvaltype varchar(250),
lediging_24h_later varchar(250),
volume_sinds_lediging varchar(250),
volume_dag varchar(250));

COPY ml_input_zonder
FROM 'C:\temp\ml_input_3.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8'; 	

select * from ml_input_zonder;		
				   
UPDATE ml_input_zonder SET container_nr=TRIM(leading '0' FROM CAST(container_nr as text));
	
-- Op de dag van een lediding worden er twee records per dag gelogged (stortingen voor en na de lediging).
-- Deze worden hier opgeteld en de overbodige record verwijderd

-- drop view ml_input_corr cascade;
create or replace view ml_input_corr as 
select datum, container_nr, container_afvaltype, 
min(lediging_24h_later) over (partition by datum, container_nr order by volume_sinds_lediging desc) as lediging_24h_later,
sum(volume_sinds_lediging::integer) over (partition by datum, container_nr order by volume_sinds_lediging desc) as volume_sinds_lediging,
sum(volume_dag::integer) over (partition by container_nr,datum order by volume_sinds_lediging desc) as volume_dag
from ml_input_zonder;

create or replace view ml_input_final as 
select datum, container_nr, container_afvaltype, lediging_24h_later, volume_sinds_lediging, volume_dag,
	rank() over (partition by container_nr, datum order by volume_sinds_lediging desc)
from ml_input_corr;

create or replace view ml_input_final_rank as 			   
select * from ml_input_final where rank = 1;
				   	   
--- Ten slotte, extra variabelen over de zuilen en buurt. Om een aantal extra variabelen mee te geven.

-- Via QGIS werd een db gekoppeld met de geografische locatie van de containers. 
-- Echter de ID's van de containers in deze tabel komen niet overeen met de ID's in de container tabel. De tabel juist is een conversie tabel.				   
---drop view zuilen cascade;				   
create table juist (
fout varchar(250),
juist varchar(250));

COPY juist
FROM 'C:\temp\juist.csv' DELIMITER ';' CSV HEADER ENCODING 'UTF8'; 	
	  
create or replace view zuilen_corr as 			  
select j.juist container_nr, j.fout id_oud, *
from export_zuilen_met_xy e
join juist j on j.fout::varchar = e.id::varchar;

select * from zuilen_corr;
			  
--- met een GIS query wordt er hier een join gemaakt met de bevolking (aantal per statsec) + hercodering van de achtergrondkenmerken van de containers = statische tabel 		  
--- drop view zuilen;
create or replace view zuilen as
SELECT x.container_nr, "FRACTIE" fractie, "VERPLICHT" verplicht, "LEDFREQ" ledfreq,  niscode, r.aantal,
		CASE WHEN "MA" = 'x' then 'ma'
			 WHEN "DI" = 'x' then 'di'
			 WHEN "WO" = 'x' then 'wo'
			 WHEN "DO" = 'x' then 'do'
			 WHEN "VR" = 'x' then 'vr' else 'afroep' END led_dag
from zuilen_corr as x, stat_sec_2019 as s
left join (select statsec, count(*) aantal from rr_2018 group by statsec) as r on r.statsec = s.niscode
WHERE ST_Within(ST_SetSRID(ST_MakePoint(x."POINT_X", x."POINT_Y"),31370) ,s.geom);

---COPY (select * from zuilen) TO 'C:\temp\zuilen.csv' DELIMITER ',' CSV HEADER;
			  
create or replace view ml_met_container as 
select m.datum, m.container_nr, m.container_afvaltype, m.lediging_24h_later, m.volume_sinds_lediging, m.volume_dag, extract(DOW from m.datum::date),
 verplicht, ledfreq, z.niscode, z.aantal, z.led_dag
from ml_input_final_rank m			   
left join zuilen z on z.container_nr::varchar = m.container_nr::varchar; 

COPY (select * from ml_met_container) TO 'C:\temp\ml_input_final_2.csv' DELIMITER ',' CSV HEADER;
				   
