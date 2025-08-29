-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD QOUTE TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_wishlist (
	wishlist_id int4 NOT NULL PRIMARY KEY
	,customer_id int4 NOT NULL
	,shared int4 NOT NULL
	,sharing_code text NOT NULL
	,updated_at timestamp NOT NULL
	,name text NULL
	,visibility int4 NOT NULL
	,"load_ts" timestamptz NULL
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_wishlist (
	wishlist_id
	,customer_id
	,shared
	,sharing_code
	,updated_at
	,name
	,visibility
	,"load_ts"
)

SELECT
	mpw.wishlist_id
	,mpw.customer_id
	,mpw.shared
	,mpw.sharing_code
	,mpw.updated_at
	,mpw.name
	,mpw.visibility
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_wishlist mpw
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_wishlist()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_wishlist (
		wishlist_id
		,customer_id
		,shared
		,sharing_code
		,updated_at
		,name
		,visibility
		,"load_ts"
	)

	VALUES (
		NEW.wishlist_id
		,NEW.customer_id
		,NEW.shared
		,NEW.sharing_code
		,NEW.updated_at
		,NEW.name
		,NEW.visibility
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("wishlist_id") DO UPDATE
	SET
		customer_id = EXCLUDED.customer_id
		,shared = EXCLUDED.shared
		,sharing_code = EXCLUDED.sharing_code
		,updated_at = EXCLUDED.updated_at
		,name = EXCLUDED.name
		,visibility = EXCLUDED.visibility
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD QOUTE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_wishlist ON bronze.magento_prod_wishlist;
CREATE TRIGGER trg_upsert_magento_prod_wishlist
AFTER INSERT OR UPDATE ON bronze.magento_prod_wishlist
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_wishlist();

