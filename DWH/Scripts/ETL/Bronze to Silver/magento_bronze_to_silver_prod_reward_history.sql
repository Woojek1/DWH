-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD REWARD HISTORY TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_reward_history (
	history_id int4 NOT NULL PRIMARY KEY
	,reward_id int4 NOT NULL
	,website_id int4 NOT NULL
	,store_id int4 NOT NULL
	,"action" int4 NOT NULL
	,entity int4 NULL
	,points_balance int4 NOT NULL
	,points_delta int4 NOT NULL
	,points_used int4 NOT NULL
	,points_voided int4 NOT NULL
	,currency_amount numeric(12, 4) NOT NULL
	,currency_delta numeric(12, 4) NOT NULL
	,base_currency_code text NOT NULL
	,additional_data json NULL
	,"comment" text NULL
	,created_at timestamp NOT NULL
	,expired_at_static timestamp NULL
	,expired_at_dynamic timestamp NULL
	,is_expired int4 NOT NULL
	,is_duplicate_of int4 NULL
	,notification_sent int4 NOT NULL
	,"load_ts" timestamptz NULL
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_reward_history (
	history_id
	,reward_id
	,website_id
	,store_id
	,"action"
	,entity
	,points_balance
	,points_delta
	,points_used
	,points_voided
	,currency_amount
	,currency_delta
	,base_currency_code
	,additional_data
	,"comment"
	,created_at
	,expired_at_static
	,expired_at_dynamic
	,is_expired
	,is_duplicate_of
	,notification_sent
	,"load_ts"

)

SELECT
	prh.history_id
	,prh.reward_id
	,prh.website_id
	,prh.store_id
	,prh."action"
	,prh.entity
	,prh.points_balance
	,prh.points_delta
	,prh.points_used
	,prh.points_voided
	,prh.currency_amount
	,prh.currency_delta
	,prh.base_currency_code
	,prh.additional_data
	,prh."comment"
	,prh.created_at
	,prh.expired_at_static
	,prh.expired_at_dynamic
	,prh.is_expired
	,prh.is_duplicate_of
	,prh.notification_sent
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_reward_history prh



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_reward_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_reward_history (
		history_id
		,reward_id
		,website_id
		,store_id
		,"action"
		,entity
		,points_balance
		,points_delta
		,points_used
		,points_voided
		,currency_amount
		,currency_delta
		,base_currency_code
		,additional_data
		,"comment"
		,created_at
		,expired_at_static
		,expired_at_dynamic
		,is_expired
		,is_duplicate_of
		,notification_sent
		,"load_ts"

	)

	VALUES (
		NEW.history_id
		,NEW.reward_id
		,NEW.website_id
		,NEW.store_id
		,NEW."action"
		,NEW.entity
		,NEW.points_balance
		,NEW.points_delta
		,NEW.points_used
		,NEW.points_voided
		,NEW.currency_amount
		,NEW.currency_delta
		,NEW.base_currency_code
		,NEW.additional_data
		,NEW."comment"
		,NEW.created_at
		,NEW.expired_at_static
		,NEW.expired_at_dynamic
		,NEW.is_expired
		,NEW.is_duplicate_of
		,NEW.notification_sent
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("history_id") DO UPDATE
	SET
		reward_id = EXCLUDED.reward_id
		,website_id = EXCLUDED.website_id
		,store_id = EXCLUDED.store_id
		,"action" = EXCLUDED."action"
		,entity = EXCLUDED.entity
		,points_balance = EXCLUDED.points_balance
		,points_delta = EXCLUDED.points_delta
		,points_used = EXCLUDED.points_used
		,points_voided = EXCLUDED.points_voided
		,currency_amount = EXCLUDED.currency_amount 
		,currency_delta = EXCLUDED.currency_delta
		,base_currency_code = EXCLUDED.base_currency_code
		,additional_data = EXCLUDED.additional_data
		,"comment" = EXCLUDED."comment"
		,created_at = EXCLUDED.created_at
		,expired_at_static = EXCLUDED.expired_at_static
		,expired_at_dynamic = EXCLUDED.expired_at_dynamic
		,is_expired = EXCLUDED.is_expired
		,is_duplicate_of = EXCLUDED.is_duplicate_of
		,notification_sent = EXCLUDED.notification_sent
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER ENTITY TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_reward_history ON bronze.magento_prod_reward_history;
CREATE TRIGGER trg_upsert_magento_prod_reward_history
AFTER INSERT OR UPDATE ON bronze.magento_prod_reward_history
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_reward_history();

