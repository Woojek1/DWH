-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD CUSTOMER ENTITY TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_customer_entity (
	"entity_id" serial4 PRIMARY KEY
	,"website_id" int2 NULL
	,"email" varchar(255) NULL
	,"group_id" int2 DEFAULT 0 NOT NULL
	,"increment_id" varchar(50) NULL
	,"store_id" int2 DEFAULT 0 NOT NULL
	,"created_at" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,"updated_at" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,"is_active" int2 DEFAULT 1 NOT NULL
	,"disable_auto_group_change" int2 DEFAULT 0 NOT NULL
	,"created_in" varchar(255) NULL
	,"prefix" varchar(40) NULL
	,"firstname" varchar(255) NULL
	,"middlename" varchar(255) NULL
	,"lastname" varchar(255) NULL
	,"suffix" varchar(40) NULL
	,"dob" date NULL
	,"password_hash" varchar(128) NULL
	,"rp_token" varchar(128) NULL
	,"rp_token_created_at" timestamp NULL
	,"default_billing" int4 NULL
	,"default_shipping" int4 NULL
	,"taxvat" varchar(50) NULL
	,"confirmation" varchar(64) NULL
	,"gender" int2 NULL
	,"failures_num" int2 DEFAULT 0 NOT NULL
	,"first_failure" timestamp NULL
	,"lock_expires" timestamp NULL
	,"session_cutoff" timestamp NULL
	,"load_ts" timestamptz NULL
);



-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_customer_entity (
	"entity_id"
	,"website_id"
	,"email"
	,"group_id"
	,"increment_id"
	,"store_id"
	,"created_at"
	,"updated_at"
	,"is_active"
	,"disable_auto_group_change"
	,"created_in"
	,"prefix"
	,"firstname"
	,"middlename"
	,"lastname"
	,"suffix"
	,"dob"
	,"password_hash"
	,"rp_token"
	,"rp_token_created_at"
	,"default_billing"
	,"default_shipping"
	,"taxvat"
	,"confirmation"
	,"gender"
	,"failures_num"
	,"first_failure"
	,"lock_expires"
	,"session_cutoff"
	,"load_ts"
)

SELECT
	pce."entity_id"
	,pce."website_id"
	,pce."email"
	,pce."group_id"
	,pce."increment_id"
	,pce."store_id"
	,pce."created_at"
	,pce."updated_at"
	,pce."is_active"
	,pce."disable_auto_group_change"
	,pce."created_in"
	,pce."prefix"
	,pce."firstname"
	,pce."middlename"
	,pce."lastname"
	,pce."suffix"
	,pce."dob"
	,pce."password_hash"
	,pce."rp_token"
	,pce."rp_token_created_at"
	,pce."default_billing"
	,pce."default_shipping"
	,pce."taxvat"
	,pce."confirmation"
	,pce."gender"
	,pce."failures_num"
	,pce."first_failure"
	,pce."lock_expires"
	,pce."session_cutoff"
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_customer_entity pce



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_customer_entity()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_customer_entity (
		"entity_id"
		,"website_id"
		,"email"
		,"group_id"
		,"increment_id"
		,"store_id"
		,"created_at"
		,"updated_at"
		,"is_active"
		,"disable_auto_group_change"
		,"created_in"
		,"prefix"
		,"firstname"
		,"middlename"
		,"lastname"
		,"suffix"
		,"dob"
		,"password_hash"
		,"rp_token"
		,"rp_token_created_at"
		,"default_billing"
		,"default_shipping"
		,"taxvat"
		,"confirmation"
		,"gender"
		,"failures_num"
		,"first_failure"
		,"lock_expires"
		,"session_cutoff"
		,"load_ts"
	)

	VALUES (
		NEW."entity_id"
		,NEW."website_id"
		,NEW."email"
		,NEW."group_id"
		,NEW."increment_id"
		,NEW."store_id"
		,NEW."created_at"
		,NEW."updated_at"
		,NEW."is_active"
		,NEW."disable_auto_group_change"
		,NEW."created_in"
		,NEW."prefix"
		,NEW."firstname"
		,NEW."middlename"
		,NEW."lastname"
		,NEW."suffix"
		,NEW."dob"
		,NEW."password_hash"
		,NEW."rp_token"
		,NEW."rp_token_created_at"
		,NEW."default_billing"
		,NEW."default_shipping"
		,NEW."taxvat"
		,NEW."confirmation"
		,NEW."gender"
		,NEW."failures_num"
		,NEW."first_failure"
		,NEW."lock_expires"
		,NEW."session_cutoff"
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("entity_id") DO UPDATE
	SET
		"website_id" = EXCLUDED."website_id"
		,"email" = EXCLUDED."email"
		,"group_id" = EXCLUDED."group_id"
		,"increment_id" = EXCLUDED."increment_id"
		,"store_id" = EXCLUDED."store_id"
		,"created_at" = EXCLUDED."created_at"
		,"updated_at" = EXCLUDED."updated_at"
		,"is_active" = EXCLUDED."is_active"
		,"disable_auto_group_change" = EXCLUDED."disable_auto_group_change"
		,"created_in" = EXCLUDED."created_in"
		,"prefix" = EXCLUDED."prefix"
		,"firstname" = EXCLUDED."firstname"
		,"middlename" = EXCLUDED."middlename"
		,"lastname" = EXCLUDED."lastname"
		,"suffix" = EXCLUDED."suffix"
		,"dob" = EXCLUDED."dob"
		,"password_hash" = EXCLUDED."password_hash"
		,"rp_token" = EXCLUDED."rp_token"
		,"rp_token_created_at" = EXCLUDED."rp_token_created_at"
		,"default_billing" = EXCLUDED."default_billing"
		,"default_shipping" = EXCLUDED."default_shipping"
		,"taxvat" = EXCLUDED."taxvat"
		,"confirmation" = EXCLUDED."confirmation"
		,"gender" = EXCLUDED."gender"
		,"failures_num" = EXCLUDED."failures_num"
		,"first_failure" = EXCLUDED."first_failure"
		,"lock_expires" = EXCLUDED."lock_expires"
		,"session_cutoff" = EXCLUDED."session_cutoff"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER ENTITY TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_customer_entity ON bronze.magento_prod_customer_entity;
CREATE TRIGGER trg_upsert_magento_prod_customer_entity
AFTER INSERT OR UPDATE ON bronze.magento_prod_customer_entity
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_customer_entity();

