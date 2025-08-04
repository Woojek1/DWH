-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD REWARD HISTORY TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_requisition_list (
	"entity_id" int4 NOT NULL PRIMARY KEY
	,"customer_id" int4 NOT NULL
	,"name" text NOT NULL
	,"description" text NULL
	,"updated_at" timestamp NOT NULL
	,"load_ts" timestamptz
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_requisition_list (
	"entity_id"
	,"customer_id"
	,"name"
	,"description"
	,"updated_at"
	,"load_ts"
)

SELECT
	prl."entity_id"
	,prl."customer_id"
	,prl."name"
	,prl."description"
	,prl."updated_at"
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_requisition_list prl



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_reward()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_reward (
		reward_id
		,customer_id
		,website_id
		,points_balance
		,website_currency_code
		,"load_ts"
	)

	VALUES (
		NEW.reward_id
		,NEW.customer_id
		,NEW.website_id
		,NEW.points_balance
		,NEW.website_currency_code
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("reward_id") DO UPDATE
	SET
		reward_id = EXCLUDED.reward_id
		,customer_id = EXCLUDED.customer_id
		,website_id = EXCLUDED.website_id
		,points_balance = EXCLUDED.points_balance
		,website_currency_code = EXCLUDED.website_currency_code
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER ENTITY TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_reward ON bronze.magento_prod_reward;
CREATE TRIGGER trg_upsert_magento_prod_reward
AFTER INSERT OR UPDATE ON bronze.magento_prod_reward
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_reward();

