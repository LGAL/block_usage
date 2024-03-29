-- OBJECTIVE
-- The prime objective is to extract the template information from the block_usage base denormalized data set
-- for some normalization and aggregation.
-- Before we'd do this we need to check the data quality of the base table, i.e. if there is any
-- so called "update anomaly", i.e. for certain IDs their attributes (measure numbers or keys)
-- are different in their different occurrences.
-- In our case here we have only the templates (as parents) where their keys and attributes are present
-- for each of their campaigns' records. This is what we call "denormalization" i.e. the result of a
-- join where the template table was joined with the campaigns table and put into one single table.
--
-- ASSUMPTIONS to check
-- 1. The block_usage data set describes campaigns where each campaign has one and only one row in the data set
-- 2. The campaigns are uniquely identified by the (env, suite_campaign_id) pairs -- we have checked and this is true!
-- 3. Each campaign uses one and only one template, where the template related attributes for the campaign's template
--    are present in each campaign row (i.e. the template information is denormalized in the data set).
-- 4. The templates are uniquely identified by the (env,template_id) pairs -- THIS NEEDS TO BE CHECKED below
-- 5. Each template belongs to one and only one customer where customers are identified by the (customer_id) single column key
--    THIS NEEDS TO BE CHECKED below.
-- 6. Each customer exists only in one env, i.e. the same customer_id does not exist in two or more environments. -- Tested and TRUE.

-- A.1
-- script to create a table for unique templates from the main denormalized table block_usage
-- where we add all the numeric values to the template ID vector (tuple) as well
create table bu_env_cust_templates_values as
select distinct
env, customer_id, template_id, template_block_templates_count, template_blocks_count, template_unique_blocks_count
from block_usage;
commit;
-- 6654 rows were created

-- A.2
-- Let's create a table for only the three main columns for identifying templates.
-- Now we exclude the numeric measure columns. Theoretically we should have the same number
-- of records if all is OK, i.e. there is no update anomaly.
-- Not sure yet if the customer_id is needed as well, will check later
create table bu_env_cust_templates as
select distinct
env, customer_id, template_id
from block_usage;
commit;
-- 6650 rows we have duplication in four cases.
-- The question is if this very small set of data errors can be ignored

-- A.3
-- let's identify those four rows where multiplication happened
select
env,customer_id,template_id, count(*)
from bu_env_cust_templates_values tv
group by env,customer_id,template_id
having count(*)>1;
-- The result is these four cases.
-- env     customer_id template_id              count
-- ------- ----------- ------------------------ -----
-- suite16	765953091	  5f840726acd86c2bebcc899e	2
-- suite16	765858378	  5b7454743344ca0004ce8ea9	2
-- suite38	799395663	  5d6931def893d50004dfec59	2
-- suite38	799945931	  606c1dfe434923d38d549a93	2

-- A.4
-- Let's see what are the differences in the pairs.
-- The details of these four duplicates are these:
WITH dups as
(
  select
  env,customer_id,template_id, count(*)
  from bu_env_cust_templates_values tv
  group by env,customer_id,template_id
  having count(*)>1
)
select tv.*
from bu_env_cust_templates_values tv
join dups
on dups.env=tv.env and dups.customer_id=tv.customer_id and dups.template_id=tv.template_id
order by tv.env, tv.customer_id, tv.template_id
;
-- The results are below
-- env	customer_id	template_id	template_block_templates_count	template_blocks_count	template_unique_blocks_count
-- suite16	765858378	5b7454743344ca0004ce8ea9	29	10	10
-- suite16	765858378	5b7454743344ca0004ce8ea9	30	10	10
-- suite16	765953091	5f840726acd86c2bebcc899e	28	22	12
-- suite16	765953091	5f840726acd86c2bebcc899e	29	22	12
-- suite38	799395663	5d6931def893d50004dfec59	34	29	29
-- suite38	799395663	5d6931def893d50004dfec59	35	29	29
-- suite38	799945931	606c1dfe434923d38d549a93	27	10	7
-- suite38	799945931	606c1dfe434923d38d549a93	27	9	6

-- B.1
-- Now let's go down to the two columns (env,template_id) that theoretically
-- should uniquely identify a template across the whole organization.
create table bu_env_templates as
select distinct
env, template_id
from block_usage;
-- 6339 Hmm, here we have 311 less records, i.e. due to customer_id multiplications we lost recrds.

-- B.2
-- Let's identify those 331 customers 
 with m as
(
 select env, template_id, count(*) as cust_count
  from bu_env_cust_templates
  group by env, template_id
  having count(*)>1
  order by env, template_id
)
select
ect.env,ect.template_id,ect.customer_id,m.cust_count
from bu_env_cust_templates ect
join m
on ect.env = m.env and ect.template_id=m.template_id
order by ect.env,ect.template_id,ect.customer_id;
-- 443 rows selected and dumpled into the multiple_customers_for_env-template_id_pairs.csv file

-- C.
-- Checking assumption 6. that the customer_id is not repeating across envs.
-- For this first we create a table for all customer_ids
create table bu_customers as
select distinct env, customer_id
from block_usage;
-- 2555 customer records are created.

-- Now let's check the 6.th assumption
select customer_id, count(env)
from bu_customers
group by customer_id
having count(env)>1;
-- no rows returned, i.e. each customer_id appears in one and only one environment.

-- 2021.11.20
-- Döntés született, hogy azokat a sorokat, amelyek megsértik a 4. és 5. feltételezést, kihagyjuk a mintából
-- hogy biztosítsuk a feltételeket a további aggregálások tisztasága végett.
-- A feltételek:
-- 4. A template-ek egyedi azonosítója az (env,template_id) rendezett pár.
-- 5. Minden template egy és csak egy customer-hez tartozik, ahol a customer a (customer_id) azonosít az összes env-en keresztül.

-- 33.309 problémás campaign sort kiszűrtünk a block_usage 663.713 sorából, így maradt 630.404 sor
-- amit egy új táblában tárolunk a további műveletekhez: block_usage_cleaned.
create table block_usage_cleaned as
(
with problematic_templates as
(select env,template_id, count(*) from bu_env_cust_templates_values
group by env,template_id
having count(*) > 1
),
problematic_campaigns as
(select distinct bu.env, bu.suite_campaign_id
from block_usage bu
join problematic_templates pt
  on bu.env=pt.env and bu.template_id=pt.template_id
)
select bu.*
from block_usage bu
left outer join problematic_campaigns pc
on (bu.env=pc.env and bu.suite_campaign_id = pc.suite_campaign_id)
where pc.suite_campaign_id is NULL
);
-- Itt is definiáljuk az elsődleges kulcsot
ALTER TABLE block_usage_cleaned
ADD PRIMARY KEY (env, suite_campaign_id);
;

-- mielőtt tovább mennénk a block_usage_cleaned-en ismét elvégezzük a fenti ellenőrzéseket,
-- hogy biztosak legyünk abban, hogy jól csináltuk.
create table bu_env_cust_templates_values_cleaned as
select distinct
env, customer_id, template_id, template_block_templates_count, template_blocks_count, template_unique_blocks_count
from block_usage_cleaned;
-- 6.213 sor jött létre  


create table bu_env_cust_templates_cleaned as
select distinct
env, customer_id, template_id
from block_usage_cleaned;
-- 6.213 sor jött létre, no ez már ígéretes

create table bu_env_templates_cleaned as
select distinct
env, template_id
from block_usage_cleaned;
-- 6.213 sor jött létre, tehát rendben vagyunk.
-- Kezdődhet a feldolgozás.

-- ===== Az első kérdés, amire keressük a választ ====
-- Mennyire hasonlít egy kampány egy template-re a felhasznált unique blockok számát tekintve?
-- -    Mekkora az eltérés a blockok számaiban %-osan (hisztogram)
-- Kampányonként megnézni a diff-et a number of blocks_tp és number of blocks_cp között -> igaz-e
-- hogy a kevés blokkú kampányok esetében kisebb az eltérés, mint a sokk blokkúaknál?
-- Először a unique blokkokra

-- Végrehajtási terv:
-- 1. A MySQL-ben nincs medián függvény, ezért a netről szedtem egy mintánt és írtam a következő függvényt:
--    tp_cp_unique_block_count_median_function.sql
--    Ezt kell lefuttatni, hogy létrejöjjön a function az adatbázisban.
-- 2. Hangolás: Ahhoz, hogy a fenti median függvényt hatékonyan lehessen használni más 
--    SQL utasításokban, egy indexet kell létrehozni a medián számításának támogatásához.
ALTER TABLE block_usage_cleaned
ADD INDEX ENV_TEMP (env ASC, template_id ASC) VISIBLE;

-- 3. Csinálunk egy bővített tempate values táblát, ami mindent tartalmaz már
--    Ez a lényege az egész manipulációnak, benne van minden okosságunk.
drop table bu_env_cust_templates_values_cleaned_x;

create table bu_env_cust_templates_values_cleaned_x as 
select 
env, customer_id, template_id, template_block_templates_count, template_blocks_count, template_unique_blocks_count, 
count(*) as campgaign_count, 
avg(campaign_unique_blocks_count) as avg_cub_count,
template_unique_blocks_count - avg(campaign_unique_blocks_count) as templ_avg_cub_count_diff,
tp_cp_ubc_med(env,template_id) as med_cub_count,
template_unique_blocks_count - tp_cp_ubc_med(env,template_id) as templ_med_cub_count_diff,
max(campaign_unique_blocks_count) as max_cub_count, 
min(campaign_unique_blocks_count) as min_cub_count, 
stddev_pop(campaign_unique_blocks_count) as stddev_pop_cub_count
from block_usage_cleaned buc 
group by env, customer_id, template_id, template_block_templates_count, template_blocks_count, template_unique_blocks_count 
;

select * from bu_env_cust_templates_values_cleaned_x;

-- 4. Erre a bu_env_cust_templates_values_cleaned_x táblára volna szükségünk,
--    de a nevét egyszerűsítendő átmásoljuk a bu_temlates táblába úgy,
--    hogy sorszám oszlopot adunk hozzá és , majd ezen dolgozunk.

drop table bu_templates;

create table bu_templates as
select
  row_number() over ( order by env,template_id ) as rn,
  x.*
from bu_env_cust_templates_values_cleaned_x x;

-- 5. (env,template_id) primary key constraint-et definiálunk a bu_templates táblán,
--    hogy védjük a konzisztenciát
ALTER TABLE `eszter`.`bu_templates` 
CHANGE COLUMN `template_id` `template_id` VARCHAR(30) NOT NULL ,
ADD PRIMARY KEY (`env`, `template_id`);

-- 6. Ellenőrzés: van-e olyan tempáltum, amihez nem számoltuk ki a mediánt?

select * from bu_templates  where med_cub_count is null order by rn asc; -- no row(s) returned. Bingo!
select * from bu_templates  where med_cub_count is not null order by rn asc; -- 6213 rows(s) returned. Bingo!
-- 6.213 sor jött vissza, azaz mindegyikhez kiszámoltuk
-- Ennek a lekérdezésnek az eredményét exportáljuk block_usage_templates_with_campgaign_stats.csv-be
-- excellel majd block_usage_templates_with_campgaign_stats.xlsx-be némi csinosítás után.
-- Innen excel elemzés jön grafikákkal, stb.

-- 7. Export
select * from bu_templates; -- majd csv és xls fájlba tesszük
