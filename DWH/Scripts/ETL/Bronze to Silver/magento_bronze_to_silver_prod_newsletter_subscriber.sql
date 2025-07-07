-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD NEWSLETTER SUBSCRIBER TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_newsletter_subscriber (
	"subscriber_id" serial4 NOT NULL
	,"store_id" int2 DEFAULT 0 NULL
	,"change_status_at" timestamp NULL
	,"customer_id" int4 DEFAULT 0 NOT NULL
	,"subscriber_email" varchar(150) NULL
	,"subscriber_status" int4 DEFAULT 0 NOT NULL
	,"subscriber_confirm_code" varchar(32) NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT magento_prod_newsletter_subscriber_pkey PRIMARY KEY ("subscriber_id")
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_newsletter_subscriber (
	"subscriber_id"
	,"store_id"
	,"change_status_at"
	,"customer_id"
	,"subscriber_email" 
	,"subscriber_status"
	,"subscriber_confirm_code"
	,"load_ts"
)

SELECT
	pns."subscriber_id"
	,pns."store_id"
	,pns."change_status_at"
	,pns."customer_id"
	,pns."subscriber_email" 
	,pns."subscriber_status"
	,pns."subscriber_confirm_code"
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_newsletter_subscriber pns



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_newsletter_subscriber()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_newsletter_subscriber (
		"subscriber_id"
		,"store_id"
		,"change_status_at"
		,"customer_id"
		,"subscriber_email" 
		,"subscriber_status"
		,"subscriber_confirm_code"
		,"load_ts"
	)

	VALUES (
		NEW."subscriber_id"
		,NEW."store_id"
		,NEW."change_status_at"
		,NEW."customer_id"
		,NEW."subscriber_email" 
		,NEW."subscriber_status"
		,NEW."subscriber_confirm_code"
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("subscriber_id") DO UPDATE
	SET
		"store_id" = EXCLUDED."store_id"
		,"change_status_at" = EXCLUDED."change_status_at"
		,"customer_id" = EXCLUDED."customer_id"
		,"subscriber_email" = EXCLUDED."subscriber_email"
		,"subscriber_status" = EXCLUDED."subscriber_status"
		,"subscriber_confirm_code" = EXCLUDED."subscriber_confirm_code"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD NEWSLETTER SUBSCRIBER
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_newsletter_subscriber ON bronze.magento_prod_newsletter_subscriber;
CREATE TRIGGER trg_upsert_magento_prod_newsletter_subscriber
AFTER INSERT OR UPDATE ON bronze.magento_prod_newsletter_subscriber
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_newsletter_subscriber();

