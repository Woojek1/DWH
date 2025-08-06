-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD REQUISITION LIST TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_requisition_list_item (
	item_id bigserial NOT null primary KEY
	,requisition_list_id int8 NOT NULL
	,sku varchar(64) NOT NULL
	,store_id int2 NULL
	,added_at timestamp NULL
	,qty numeric(12, 4) NOT NULL
	,"options" text NULL
	,"load_ts" timestamptz
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_requisition_list_item (
	item_id
	,requisition_list_id
	,sku
	,store_id
	,added_at
	,qty
	,"options"
	,"load_ts"
)

SELECT
	prli.item_id
	,prli.requisition_list_id
	,prli.sku
	,prli.store_id
	,prli.added_at
	,prli.qty
	,prli."options"
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_requisition_list_item prli



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_requisition_list_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_requisition_list_item (
		item_id
		,requisition_list_id
		,sku
		,store_id
		,added_at
		,qty
		,"options"
		,"load_ts"
	)

	VALUES (
		NEW.item_id
		,NEW.requisition_list_id
		,NEW.sku
		,NEW.store_id
		,NEW.added_at
		,NEW.qty
		,NEW."options"
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("item_id") DO UPDATE
	SET
		requisition_list_id = EXCLUDED.requisition_list_id
		,sku = EXCLUDED.sku
		,store_id = EXCLUDED.store_id
		,added_at = EXCLUDED.added_at
		,qty = EXCLUDED.qty
		,"options" = EXCLUDED."options"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD REQUISITION LIST TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_requisition_list_item ON bronze.magento_prod_requisition_list_item;
CREATE TRIGGER trg_upsert_magento_prod_requisition_list_item
AFTER INSERT OR UPDATE ON bronze.magento_prod_requisition_list_item
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_requisition_list_item();

