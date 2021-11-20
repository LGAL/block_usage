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
campaign_unique_blocks_count

select * from