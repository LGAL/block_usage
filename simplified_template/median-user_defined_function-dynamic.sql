DELIMITER $$;
CREATE FUNCTION MEDIAN_D (in_table_name VARCHAR(100), in_column_name varchar(100))
RETURNS INTEGER
DETERMINISTIC
BEGIN 
DECLARE v_median INTEGER;

SET @rowindex := -1;
-- SET @dny_sql = CONCAT(
SELECT
   AVG(t.@in_column_name) into v_median
FROM
  ( SELECT @rowindex:=@rowindex + 1 AS rowindex, @in_column_name
    FROM @in_table_name where @in_column_name is not null
    ORDER BY @in_column_name
  ) AS t
WHERE
t.rowindex IN (FLOOR(@rowindex / 2), CEIL(@rowindex / 2));

-- PREPARE stmt FROM @dyn_sql;
-- EXECUTE stmt;
-- DEALLOCATE PREPARE stmt;

RETURN v_median;
END;


Error Code: 1064. You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '@in_column_name) into v_median FROM   ( SELECT @rowindex:=@rowindex + 1 AS rowin' at line 10
