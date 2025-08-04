-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD CUSTOMER LOG TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_event_logger (
	event_id int4 NOT NULL PRIMARY KEY
	,user_id int4 NOT NULL
	,event_type varchar(50) NOT NULL
	,event_value varchar(255) NULL
	,event_time timestamp NOT NULL
	,"load_ts" timestamptz NULL
	);



-- Pierwsze ładowanie danych z bronze do silver	

INSERT INTO silver.magento_prod_event_logger (
	event_id
	,user_id
	,event_type
	,event_value
	,event_time
	,"load_ts"
)

SELECT
	pel.event_id
	,pel.user_id
	,pel.event_type
	,pel.event_value
	,pel.event_time
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_event_logger pel



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_event_logger ()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_event_logger (
		event_id
		,user_id
		,event_type
		,event_value
		,event_time
		,"load_ts"
	)

	VALUES (
		NEW.event_id
		,NEW.user_id
		,NEW.event_type
		,NEW.event_value
		,NEW.event_time
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("event_id") DO UPDATE
	SET
		user_id = EXCLUDED.user_id
		,event_type = EXCLUDED.event_type
		,event_value = EXCLUDED.event_value
		,event_time = EXCLUDED.event_time
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER LOG TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_event_logger ON bronze.magento_prod_event_logger;
CREATE TRIGGER trg_upsert_magento_prod_event_logger
AFTER INSERT OR UPDATE ON bronze.magento_prod_event_logger
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_event_logger();

