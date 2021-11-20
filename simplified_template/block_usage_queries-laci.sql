-- This is a notebook of multiple queries examining the block_usage table
-- Percentile example

with percentiles as
  (
  select env, suite_campaign_id, ntile(100) over (ORDER by campaign_unique_blocks_count) as pctl
    from eszter.block_usage
  )
select pctl as percentile, count(*)
from percentiles
group by pctl
order by pctl desc
;

-- Reference: https://stackoverflow.com/questions/1764881/getting-data-for-histogram-plot
 WITH recursive numbers AS (
    select 0 as cnt
   union all
   select cnt + 1
   from numbers
   where cnt < 54),
buckets as
(
select campaign_unique_blocks_count as cubc , count(*) as num_campaigns
from eszter.block_usage
where campaign_unique_blocks_count is not null
group by cubc
order by cubc
)
select cnt as unique_block_count,
       ifnull(num_campaigns,0),
       ifnull(round(10*LN(num_campaigns),2),0) as barlength,
       ifnull(RPAD('', 10*LN(num_campaigns), '*'),'') AS bar
from numbers
left outer join buckets
  on numbers.cnt=buckets.cubc
;

-- template_block_templates_count histogram
 WITH recursive numbers AS (
    select 0 as cnt
   union all
   select cnt + 1
   from numbers
   where cnt < 260),
buckets as
(
select campaign_blocks_count as cubc , count(*) as num_campaigns
from eszter.block_usage
where campaign_unique_blocks_count is not null
group by cubc
order by cubc
)
select cnt as block_count,
       ifnull(num_campaigns,0),
       ifnull(round(10*LN(num_campaigns),2),0) as barlength,
       ifnull(RPAD('', 10*LN(num_campaigns), '*'),'') AS bar
from numbers
left outer join buckets
  on numbers.cnt=buckets.cubc
;
;


-- Reference: https://stackoverflow.com/questions/10922064/how-do-i-get-a-list-of-numbers-in-mysql
 WITH recursive numbers AS (
    select 0 as cnt
   union all
   select cnt + 1
   from numbers
   where cnt < 54)
select * from numbers;

-- =============
select campaign_unique_blocks_count, count(*)
from eszter.block_usage 
group by campaign_unique_blocks_count
order by campaign_unique_blocks_count asc
;

select 
-- created_at, updated_at, launch_time, env, customer_id, suite_campaign_id, campaign_type, template_id
*
from eszter.block_usage where campaign_unique_blocks_count in (0,1,2,3,4);
---
alter table eszter.block_usage
add block_templates_diff int GENERATED ALWAYS AS ( template_block_templates_count - campaign_block_templates_count)
VIRTUAL;

alter table eszter.block_usage
add block_templates_diff int GENERATED ALWAYS AS ( template_block_templates_count - campaign_block_templates_count)
VIRTUAL;

select
round(100*(campaign_unique_blocks_count/template_unique_blocks_count),0)
from eszter.block_usage;


select * from eszter.block_usage
where (template_block_templates_count - campaign_block_templates_count) < 0
;

-- I oszlop minusz L oszlop, azaz block_templates_diff eloszlása.
-- ez csak azokat mutatja meg, ahol mindkét oszlopban van érték,
-- azaz ahol a template_block_templates_count NULL érték, 
-- azokra nem jelenik meg semmi, mert ott a virtuális oszlopi is NULL
select block_templates_diff, count(*)
from eszter.block_usage
where block_templates_diff < 0
group by block_templates_diff
order by block_templates_diff desc;

select * from eszter.block_usage
where template_block_templates_count is null ;

---
SELECT 
--     min(template_block_templates_count),
--     max(template_block_templates_count),
--     avg(template_block_templates_count),
--     median
--     std(template_block_templates_count)
max(template_block_templates_count),
-- max(template_blocks_count) ,
max(template_unique_blocks_count),
max(campaign_block_templates_count),
-- max(campaign_blocks_count),
max(campaign_unique_blocks_count),
--     block_templates_diff
'a'
FROM
    eszter.block_usage;
    
with pctrank as
(select
env,
template_block_templates_count,
suite_campaign_id,
ROUND(
   PERCENT_RANK() OVER (
      ORDER BY template_block_templates_count
   )
,2) percentile_rank
from eszter.block_usage
where template_block_templates_count is not null
),
select percentile_rank, count(*)
from pctrank
group by percentile_rank
order by percentile_rank
;

-- references:
-- CUME_DIST: https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_cume-dist
-- PERCENT_RANK: https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html#function_percent-rank
select distinct
template_block_templates_count,
ROUND(
   PERCENT_RANK() OVER (
      ORDER BY template_block_templates_count
   )
,2) percentile_rank
from eszter.block_usage
where template_block_templates_count is not null
;

select distinct
template_block_templates_count,
PERCENT_RANK() OVER w as "percent_rank",
CUME_DIST() OVER w as "cume_dist"
from eszter.block_usage
where template_block_templates_count is not null
WINDOW w AS (order by template_block_templates_count)
order by template_block_templates_count
;


with a as
(
SELECT
  ROW_NUMBER() OVER w AS 'rn',
  template_block_templates_count,
  NTILE(100)     OVER w AS 'pctl'
FROM eszter.block_usage
where template_block_templates_count is not null
WINDOW w AS (ORDER BY template_block_templates_count)
)
select distinct 
template_block_templates_count,
pctl
from a
order by template_block_templates_count;

select * from a
where rn = (select min(rn) from a where pctl >=95)
;
--
-- Hány kampány épül egy template-re?
WITH cnum_per_template as
  ( select env, template_id, count(*) as num_campaigns
    FROM eszter.block_usage
    group by env, template_id
  )
select
num_campaigns, count(*)
from cnum_per_template
group by num_campaigns
order by num_campaigns;

WITH cnum_per_template as
  ( select env, template_id, count(*) as num_campaigns
    FROM eszter.block_usage
    group by env, template_id
  )
select
avg(num_campaigns),
med(num_campaigns)
from cnum_per_template
;

select env, template_id, count(*) as num_campaigns
    FROM eszter.block_usage
    group by env, template_id
    order by count(*) desc;

with pctrank as
(select
env,
template_block_templates_count,
suite_campaign_id,
ROUND(
   PERCENT_RANK() OVER (
      ORDER BY template_block_templates_count
   )
,2) percentile_rank
from eszter.block_usage
where template_block_templates_count is not null
)
select
distinct template_block_templates_count from pctrank
 where percentile_rank = 0.49
;

select
count(*) from eszter.block_usage
where template_block_templates_count < 37;
-- 318732
-- 329450
-- 327648
-- -----
-- Calculating median via SQL in MySQL

SET @rowindex := -1;
SELECT
    AVG(t.template_block_templates_count) as Median_template_block_templates_count
FROM
  ( SELECT @rowindex:=@rowindex + 1 AS rowindex, template_block_templates_count
    FROM eszter.block_usage where template_block_templates_count is not null
    ORDER BY template_block_templates_count   
  ) AS t
WHERE
t.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2));

select @dyn_sql;

SET @rowindex := -1;
SET @col := 'template_block_templates_count';
set @dyn_sql := CONCAT(
'SELECT 
    AVG(t.',@col,') as Median_',@col,
' FROM
  ( SELECT @rowindex:=@rowindex + 1 AS rowindex, ',@col,
'    FROM eszter.block_usage where ',@col,' is not null
    ORDER BY ',@col,
'  ) AS t
WHERE
t.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2))
');
PREPARE stmt FROM @dyn_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- ------------------
-- Első kérdés
-- ------------------
-- Mennyire hasonlít egy kampány egy template-re a felhasznált unique blockok számát tekintve?
-- -    Mekkora az eltérés a blockok számaiban %-osan (hisztogram)
-- * Kampányonként megnézni a diff-et a number of blocks_tp és number of blocks_cp között -> *
-- * igaz-e hogy a kevés blokkú kampányok esetében kisebb az eltérés, mint a sokk blokkúaknál? *
-- Először a unique blokkokra
