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
-- 2. The campaigns are uniquiely identified by the (env, suite_campaign_id) pairs -- we have checked and this is true!
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