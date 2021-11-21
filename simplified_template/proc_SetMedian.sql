drop procedure SetMedian;
delimiter //
CREATE PROCEDURE SetMedian (IN in_loopsize INT)
-- This procedure is created to set the tp_cp_ubc_med median value
--  in bu_templates table based on the campgaings belonging to the template
  BEGIN
	DECLARE x  INTEGER;
  DECLARE beg INTEGER;
  DECLARE fin INTEGER;
  
  SELECT max(rn)+1 into beg from bu_templates where med_cub_count is not null;
	SET x = beg;
  SET fin = beg + in_loopsize -1;
     loop_setmedian:  LOOP
       IF  x > fin THEN
         COMMIT;
         LEAVE  loop_setmedian;
       END  IF;              
       update bu_templates set med_cub_count = tp_cp_ubc_med(env,template_id) where rn=x and med_cub_count is null;
       SET  x = x + 1;
     END LOOP;
  END//
