-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------

-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------
;/** /--/ **/ SET NAMES utf8;
-- ---------------------------------------------------------------------------------------------------------------------------


-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------
-- LOAD_ADH_ARRIVAL_FACTS_BY_TIME:
-- ---------------------------------------------------------------------------------------------------------------------------
;/** /--/ **/ DROP PROCEDURE IF EXISTS LOAD_ADH_ARRIVAL_FACTS_BY_TIME;
-- ---------------------------------------------------------------------------------------------------------------------------
delimiter ;;
  /** /--/ **/ CREATE PROCEDURE LOAD_ADH_ARRIVAL_FACTS_BY_TIME(i_min_time TIMESTAMP, i_max_time TIMESTAMP)

BEGIN


    DECLARE v_log_source VARCHAR(63) DEFAULT 'LOAD_ADH_ARRIVAL_FACTS_BY_TIME';
    DECLARE v_in_transaction TINYINT(1) DEFAULT FALSE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        IF v_in_transaction THEN
            ROLLBACK;
        END IF;
        CALL ADD_LOG_MESSAGE('ERROR', v_log_source, CONCAT('EXIT: failed for min_time=[',IFNULL(i_min_time,''),'] max_ts=[',IFNULL(i_max_time,''),']'));
        RESIGNAL;
    END;
    -- --------------------------------------------------------------
    -- --------------------------------------------------------------
	-- time offset!
		CALL utils.OFFSET_DST_FEED_TIMESTAMP_RANGE(i_min_time, i_max_time);
    -- --------------------------------------------------------------
    -- --------------------------------------------------------------

    -- --------------------------------------------------------------
    -- --------------------------------------------------------------
    -- USE A TRANSACTION FOR BETTER TRANSACTION ISOLATION
    --
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    START TRANSACTION;
    --
    SET v_in_transaction := TRUE;
    --

    INSERT INTO arrival_facts
    SELECT t.* 
      FROM (
    SELECT a.ad_id
		, a.arrival_context_id	
		, a.arrival_id
		, a.arrival_uri_id
		, a.arrivals
		, a.auto_id
		, a.client_id
		, a.ip_id
		, a.keyword_id
		, a.redirect_uri_id
		, a.tid
		, FROM_UNIXTIME(a.tsecs) tsecs
		, a.user_agent_id
		, a.referer_id
      FROM ah_fact$federated.arrival_facts a
	WHERE FROM_UNIXTIME(a.tsecs) >= i_min_time AND FROM_UNIXTIME(a.tsecs) < i_max_time
		) t
     WHERE NOT EXISTS (
    SELECT NULL 
       FROM arrival_facts i
     WHERE i.ad_id         <=> t.ad_id  
       AND i.arrival_context_id      <=> t.arrival_context_id            
       AND i.arrival_id = t.arrival_id  
       AND i.arrival_uri_id <=> t.arrival_uri_id   
       AND i.arrivals       <=> t.arrivals          
       AND i.keyword_id     <=> t.keyword_id  
       AND i.auto_id <=>t.auto_id  
       AND i.client_id <=> t.client_id   
       AND i.ip_id      <=> t.ip_id          
       AND i.redirect_uri_id       <=> t.redirect_uri_id          
       AND i.tid     <=> t.tid       
       AND i.tsecs     <=> t.tsecs 
       AND i.user_agent_id <=> t.user_agent_id  
       AND i.referer_id <=>t.referer_id   
		)
  ORDER BY arrival_id
        ON DUPLICATE KEY
    UPDATE ad_id         = t.ad_id
         , arrival_context_id = t.arrival_context_id
         , arrival_id = t.arrival_id
         , arrival_uri_id      = t.arrival_uri_id
         , arrivals       = t.arrivals
         , keyword_id     = t.keyword_id
         , auto_id = t.auto_id
         , client_id = t.client_id
         , ip_id      = t.ip_id
         , redirect_uri_id       = t.redirect_uri_id
         , tid     = t.tid
         , tsecs     = t.tsecs
         , user_agent_id = t.user_agent_id
         , referer_id = t.referer_id
    ;

	--
    COMMIT; -- ENDS TRANSACTION
	--

   SET v_in_transaction := FALSE;
    --
    -- --------------------------------------------------------------
    -- --------------------------------------------------------------


END
;;

delimiter ;
    -- --------------------------------------------------------------
    -- --------------------------------------------------------------

    -- --------------------------------------------------------------
    -- --------------------------------------------------------------
	--
    -- --------------------------------------------------------------
	; /** /--/ **/ DROP PROCEDURE IF EXISTS LOAD_ARRIVAL_FACT_BY_TIME_AND_STEP;
    -- --------------------------------------------------------------
	delimiter ;;
		/** /--/ **/ CREATE PROCEDURE LOAD_ARRIVAL_FACT_BY_TIME_AND_STEP(i_first_time TIMESTAMP, i_last_time TIMESTAMP, i_step_hr INT)


  
BEGIN
	
	DECLARE v_log_source VARCHAR(64) DEFAULT 'LOAD_ARRIVAL_FACT_BY_TIME_AND_STEP';

	DECLARE v_zero_tsecs	INT DEFAULT 0;
	
	DECLARE v_step_sec		INT DEFAULT 3600 * IFNULL(i_step_hr, 1);

	DECLARE v_last_tsecs	INT DEFAULT IF(i_last_time < 0, 0, i_last_time);
	DECLARE v_max_rows		INT 	DEFAULT 25000;
	DECLARE v_curr_tsecs	INT;
	DECLARE v_curr_id		INT;
	DECLARE v_min_time		INT;
	DECLARE v_max_tsecs		INT;
	DECLARE v_delta_sec		INT;
	DECLARE v_count			INT;
	DECLARE v_loop_count	INT DEFAULT 0;


	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		CALL ADD_LOG_MESSAGE('ERROR', v_log_source, CONCAT('EXIT: failed for first_tsecs=[',IFNULL(i_first_time,''),'], last_tsecs=[',IFNULL(i_last_time,''),'], step_hr=[',IFNULL(i_step_hr,''),']'));
		CALL ADD_LOG_MESSAGE('ERROR', v_log_source, CONCAT('EXIT: failed at curr_tsecs=[',IFNULL(v_curr_tsecs,''),'], curr_id=[',IFNULL(v_curr_id,''),'], step_hr=[',IFNULL(i_step_hr,''),'], min_time=[',IFNULL(v_min_time,''),'], max_tsecs=[',IFNULL(v_max_tsecs,''),'],'));

		RESIGNAL;
	END;

	CALL ADD_LOG_MESSAGE('LOG', v_log_source, CONCAT('BEGIN: first_tsecs=[',IFNULL(i_first_time,''),'], last_tsecs=[',IFNULL(i_last_ts,''),'], step_hr=[',IFNULL(i_step_hr,''),']'));

	SELECT IFNULL(MAX(id), 0), IFNULL(MAX(tsecs), v_zero_tsecs) INTO v_curr_id, v_curr_tsecs FROM ah_fact$federated.arrival_facts;

	IF v_curr_tsecs = v_zero_tsecs THEN
	-- SELECT IFNULL(MAX(min_updated_tsecs), 0)		FROM ah_fact$federated.arrival_facts
	SELECT IFNULL(MAX(min_updated_tsecs), v_zero_tsecs) INTO v_curr_tsecs FROM ah_fact$federated.arrival_facts;
		END IF;

	IF v_last_tsecs IS NULL THEN
	-- SELECT IFNULL(MAX(max_updated_tsecs), 0)		FROM ah_fact$federated.arrival_facts
	SELECT IFNULL(MAX(max_updated_tsecs), v_curr_tsecs) INTO v_last_tsecs FROM ah_fact$federated.arrival_facts;
		END IF;

	SET v_min_time := IF(i_first_time <= v_zero_tsecs, SUBDATE(v_curr_tsecs, INTERVAL v_step_sec SECOND), i_first_time);


    -- --------------------------------------------------------------
    -- --------------------------------------------------------------


		CALL ADD_LOG_MESSAGE('LOG', v_log_source, CONCAT('INIT: min_ts=[',IFNULL(v_min_time,''),'], step_sec=[',IFNULL(v_step_sec,''),']; curr_ts=[',IFNULL(v_curr_tsecs,''),'], curr_id=[',IFNULL(v_curr_id,''),']'));

			the_loop: LOOP

				SET v_delta_sec := v_step_sec;
				SET v_count	:= NULL;

				delta_loop: LOOP

				CALL ADD_LOG_MESSAGE('DEBUG', v_log_source, CONCAT('TRY:  loop_count=[',IFNULL(v_loop_count,''),'], count=[',IFNULL(v_count,''),'] delta_sec=[',IFNULL(v_delta_sec,''),'] min_time=[',IFNULL(v_min_time,''),'] max_tsecs=[',IFNULL(v_max_tsecs,''),']'));

			SET v_max_tsecs := ADDDATE(v_min_time, INTERVAL v_delta_sec SECOND);
			IF v_max_tsecs >= v_last_tsecs THEN SET v_max_tsecs := ADDDATE(v_last_tsecs, INTERVAL 1 SECOND); END IF;

			IF v_min_time >= v_max_tsecs THEN LEAVE the_loop; END IF;

		-- SELECT COUNT(*) FROM ah_fact$federated.arrival_facts a WHERE a.tsecs >= v_min_time AND c.tsecs < v_max_tsecs

			IF v_count = 0
			THEN 

				SET v_min_time := v_max_tsecs;
				SET v_delta_sec := v_step_sec;

				IF v_min_time > v_last_tsecs THEN LEAVE the_loop; END IF;

				ELSEIF v_count > v_max_rows AND v_delta_sec >= 3
				THEN 
					SET v_delta_sec := GREATEST(2, ROUND(v_delta_sec * LEAST(0.500, (v_max_rows/v_count)), 0));

				ELSE
					LEAVE delta_loop;
				END IF;

			END LOOP delta_loop;

			CALL ADD_LOG_MESSAGE('DEBUG', v_log_source, CONCAT('CALL: loop_count=[',IFNULL(v_loop_count,''),'], min_ts=[',IFNULL(v_min_time,''),'], max_ts=[',IFNULL(v_max_tsecs,''),']; last_ts = [',IFNULL(v_last_tsecs,''),']'));
			CALL LOAD_ADH_ARRIVAL_FACTS_BY_TIME(v_min_time, v_max_tsecs);

				SET v_min_time := SUBDATE(v_max_tsecs, INTERVAL 1 SECOND);

				IF v_min_time >= v_last_tsecs THEN LEAVE the_loop; END IF;

				SET v_loop_count := 1 + v_loop_count;

				IF MOD(v_loop_count, 24) = 0
				THEN
					CALL ADD_LOG_MESSAGE('LOG', v_log_source, CONCAT('LOOP: loop_count=[',IFNULL(v_loop_count,''),'], min_time=[',IFNULL(v_min_time,''),'], max_tsecs=[',IFNULL(v_max_tsecs,''),']; last_tsecs = [',IFNULL(v_last_tsecs,''),']'));
				END IF;

			END LOOP;
	
			COMMIT;

		CALL ADD_LOG_MESSAGE('LOG', v_log_source, 'FINISH');

END
;;
delimiter ;
-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------

-- TRUNCATE LOG_MESSAGE; ALTER TABLE LOG_MESSAGE AUTO_INCREMENT = 1;
-- SELECT * FROM V_LOG_MESSAGE_TAIL;
-- SELECT * FROM LOG_MESSAGE WHERE source = 'LOAD_ADH_TRACK_CALLBACK_BY_TS_RANGE_AND_STEP' ORDER BY id DESC LIMIT 100;

-- SELECT * FROM ah_fact$federated.arrival_facts ORDER BY arrival_id 