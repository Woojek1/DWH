-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD REQUISITION LIST TABLES IN SILVER LAYER AND FIRST LOAD
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


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_requisition_list()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_requisition_list (
		"entity_id"
		,"customer_id"
		,"name"
		,"description"
		,"updated_at"
		,"load_ts"
	)

	VALUES (
		NEW."entity_id"
		,NEW."customer_id"
		,NEW."name"
		,NEW."description"
		,NEW."updated_at"
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("entity_id") DO UPDATE
	SET
		"entity_id" = EXCLUDED."entity_id"
		,"customer_id" = EXCLUDED."customer_id"
		,"name" = EXCLUDED."name"
		,"description" = EXCLUDED."description"
		,"updated_at" = EXCLUDED."updated_at"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD REQUISITION LIST TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_requisition_list ON bronze.magento_prod_requisition_list;
CREATE TRIGGER trg_upsert_magento_prod_requisition_list
AFTER INSERT OR UPDATE ON bronze.magento_prod_requisition_list
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_requisition_list();



DROP TRIGGER IF EXISTS trg_upsert_magento_prod_wishlist ON bronze.magento_prod_wishlist;
CREATE TRIGGER trg_upsert_magento_prod_wishlist
AFTER INSERT OR UPDATE ON bronze.magento_prod_wishlist
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_wishlist();


