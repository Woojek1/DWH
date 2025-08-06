-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD WISHLIST ITEM OPTION TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_wishlist_item_option (
	option_id bigserial NOT NULL PRIMARY KEY
	,wishlist_item_id int8 NOT NULL
	,product_id int8 NOT NULL
	,code varchar(255) NOT NULL
	,value text NULL
	,"load_ts" timestamptz NULL
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_wishlist_item_option (
	option_id
	,wishlist_item_id
	,product_id
	,code
	,value
	,"load_ts"
)

SELECT
	mpwio.option_id
	,mpwio.wishlist_item_id
	,mpwio.product_id
	,mpwio.code
	,mpwio.value
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_wishlist_item_option mpwio
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_wishlist_item_option()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_wishlist_item_option (
		option_id
		,wishlist_item_id
		,product_id
		,code
		,value
		,"load_ts"
	)

	VALUES (
		NEW.option_id
		,NEW.wishlist_item_id
		,NEW.product_id
		,NEW.code
		,NEW.value
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("option_id") DO UPDATE
	SET
		option_id = EXCLUDED.option_id
		,wishlist_item_id = EXCLUDED.wishlist_item_id
		,product_id = EXCLUDED.product_id
		,code = EXCLUDED.code
		,value = EXCLUDED.value
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD WISHLIST ITEM OPTION
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_wishlist_item_option ON bronze.magento_prod_wishlist_item_option;
CREATE TRIGGER trg_upsert_magento_prod_wishlist_item_option
AFTER INSERT OR UPDATE ON bronze.magento_prod_wishlist_item_option
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_wishlist_item_option();
