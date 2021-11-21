DELIMITER $$;
CREATE FUNCTION MEDIAN (p_table_name VARCHAR(100), p_column_name varchar(100))
RETURNS INTEGER
DETERMINISTIC
BEGIN 
DECLARE v_median INTEGER;

SET @rowindex := -1;
SELECT
   AVG(t.template_block_templates_count) into v_median
FROM
  ( SELECT @rowindex:=@rowindex + 1 AS rowindex, template_block_templates_count
    FROM eszter.block_usage where template_block_templates_count is not null
    ORDER BY template_block_templates_count   
  ) AS t
WHERE
t.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2));
RETURN v_median;
END;
