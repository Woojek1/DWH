-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD WISHLIST ITEM TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_wishlist_item (
	wishlist_item_id bigserial NOT NULL PRIMARY KEY
	,wishlist_id int8 DEFAULT 0 NOT NULL
	,product_id int8 DEFAULT 0 NOT NULL
	,store_id int2 NULL
	,added_at timestamp NULL
	,description text NULL
	,qty numeric(12, 4) NOT NULL
	,"load_ts" timestamptz NULL
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_wishlist_item (
	wishlist_item_id
	,wishlist_id
	,product_id
	,store_id
	,added_at
	,description
	,qty
	,"load_ts"
)

SELECT
	mpwi.wishlist_item_id
	,mpwi.wishlist_id
	,mpwi.product_id
	,mpwi.store_id
	,mpwi.added_at
	,mpwi.description
	,mpwi.qty
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_wishlist_item mpwi
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_wishlist_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_wishlist_item (
		wishlist_item_id
		,wishlist_id
		,product_id
		,store_id
		,added_at
		,description
		,qty
		,"load_ts"
	)

	VALUES (
		NEW.wishlist_item_id
		,NEW.wishlist_id
		,NEW.product_id
		,NEW.store_id
		,NEW.added_at
		,NEW.description
		,NEW.qty
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("wishlist_item_id") DO UPDATE
	SET
		wishlist_id = EXCLUDED.wishlist_id
		,product_id = EXCLUDED.product_id
		,store_id = EXCLUDED.store_id
		,added_at = EXCLUDED.added_at
		,description = EXCLUDED.description
		,qty = EXCLUDED.qty
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD WISHLIST ITEM
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_wishlist_item ON bronze.magento_prod_wishlist_item;
CREATE TRIGGER trg_upsert_magento_prod_wishlist_item
AFTER INSERT OR UPDATE ON bronze.magento_prod_wishlist_item
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_wishlist_item();

