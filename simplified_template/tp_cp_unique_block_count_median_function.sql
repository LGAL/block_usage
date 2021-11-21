DELIMITER //;
CREATE FUNCTION TP_CP_UBC_MED (in_env VARCHAR(20), in_template_id varchar(30))
-- Ez a függvény visszaadja egy adott (env,template_id) párra a hozzá tartozó
-- összes kampány unique_block_count értékének mediánját.
RETURNS INTEGER
DETERMINISTIC
BEGIN 
DECLARE v_median INTEGER;

SET @rowindex := -1;

SELECT
   AVG(tw.campaign_unique_blocks_count) into v_median
FROM
  ( SELECT @rowindex:=@rowindex + 1 AS rowindex, campaign_unique_blocks_count
    FROM eszter.block_usage_cleaned
    where env=in_env and template_id=in_template_id -- a template csúszó ablakot határozza meg
    ORDER BY campaign_unique_blocks_count   
  ) AS tw
WHERE
tw.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2));

RETURN v_median;
END;

