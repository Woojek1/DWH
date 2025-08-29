-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD CUSTOMER ENTITY TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_inventory_source_item (
	source_item_id bigserial NOT NULL
	,source_code text NOT NULL
	,sku text NOT NULL
	,quantity numeric(12, 4) NOT NULL
	,status int2 NOT NULL
	,"load_ts" timestamptz NULL
	,PRIMARY KEY (source_item_id)
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_inventory_source_item (
	source_item_id
	,source_code
	,sku
	,quantity
	,status
	,"load_ts"
)

SELECT
	pisi.source_item_id
	,pisi.source_code
	,pisi.sku
	,pisi.quantity
	,pisi.status
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_inventory_source_item pisi



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_inventory_source_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_inventory_source_item (
	source_item_id
	,source_code
	,sku
	,quantity
	,status
	,"load_ts"
	)

	VALUES (
		NEW.source_item_id
		,NEW.source_code
		,NEW.sku
		,NEW.quantity
		,NEW.status
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("source_item_id") DO UPDATE
	SET
		source_code = EXCLUDED.source_code
		,sku = EXCLUDED.sku
		,quantity = EXCLUDED.quantity
		,status = EXCLUDED.status
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER ENTITY TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_inventory_source_item ON bronze.magento_prod_inventory_source_item;
CREATE TRIGGER trg_upsert_magento_prod_inventory_source_item
AFTER INSERT OR UPDATE ON bronze.magento_prod_inventory_source_item
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_inventory_source_item();

