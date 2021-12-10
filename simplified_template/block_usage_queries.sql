-- Hány darab blockot használnak összesen egy kampányban?
-- -	Átlagosan: 9.4894
-- -	P90
-- -	P95
-- -	Min 1-20 + melyik az
-- -	Max 1-20 + melyik az
-- -------------------------------------------------------
-- van-e duplikáció az env-cp_id párosban?
select env, suite_campaign_id, count(*)
from eszter.block_usage
group by env, suite_campaign_id
having count(*)>1;

-- átlag unique block darabszám
select avg(campaign_unique_blocks_count)
from eszter.block_usage;

-- unique block darabszám eloszlás 
select campaign_unique_blocks_count, count(*)
from eszter.block_usage 
group by campaign_unique_blocks_count
order by campaign_unique_blocks_count asc
;

-- 2021.12.10
-- unique block darabszám templateben eloszlás  
select template_unique_blocks_count, count(distinct concat( template_id,env))
from eszter.block_usage
where template_unique_blocks_count is not NULL
group by template_unique_blocks_count
order by template_unique_blocks_count asc
;

select template_unique_blocks_count, count(distinct concat( template_id,env))
from eszter.block_usage_cleaned
where template_unique_blocks_count is not NULL
group by template_unique_blocks_count
order by template_unique_blocks_count asc
;