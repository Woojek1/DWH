-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD CUSTOMER LOG TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_customer_log (
	log_id serial4 NOT NULL
	,customer_id int4 NOT NULL
	,last_login_at timestamp NULL
	,last_logout_at timestamp NULL
	,CONSTRAINT magento_prod_customer_log_customer_id_key UNIQUE (customer_id)
	,CONSTRAINT magento_prod_customer_log_pkey PRIMARY KEY (log_id)
	,"load_ts" timestamptz NULL
);



-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_customer_log (
	"log_id"
	,"customer_id"
	,"last_login_at"
	,"last_logout_at"
	,"load_ts"
)

SELECT
	pcl."log_id"
	,pcl."customer_id"
	,pcl."last_login_at"
	,pcl."last_logout_at"
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_customer_log pcl



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_customer_log ()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_customer_log (
		"log_id"
		,"customer_id"
		,"last_login_at"
		,"last_logout_at"
		,"load_ts"
	)

	VALUES (
		NEW."log_id"
		,NEW."customer_id"
		,NEW."last_login_at"
		,NEW."last_logout_at"
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("log_id") DO UPDATE
	SET
		"customer_id" = EXCLUDED."customer_id"
		,"last_login_at" = EXCLUDED."last_login_at"
		,"last_logout_at" = EXCLUDED."last_logout_at"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER LOG TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_customer_log ON bronze.magento_prod_customer_log;
CREATE TRIGGER trg_upsert_magento_prod_customer_log
AFTER INSERT OR UPDATE ON bronze.magento_prod_customer_log
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_customer_log();

