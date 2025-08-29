-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD CUSTOMER ENTITY TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_catalog_product_entity (
	entity_id int8 NOT NULL
	,attribute_set_id int4 NOT NULL
	,type_id varchar(32) NOT NULL
	,sku varchar(64) NOT NULL
	,has_options int2 DEFAULT 0 NOT NULL
	,required_options int2 DEFAULT 0 NOT NULL
	,created_at timestamp NULL
	,updated_at timestamp NULL
	,row_id int8 NOT NULL
	,created_in int4 NOT NULL
	,updated_in int4 NOT NULL
	,"load_ts" timestamptz NULL
	,PRIMARY KEY (entity_id)

);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_catalog_product_entity (
	entity_id
	,attribute_set_id
	,type_id
	,sku
	,has_options
	,required_options
	,created_at
	,updated_at
	,row_id
	,created_in
	,updated_in
	,load_ts
)

SELECT
	cpe.entity_id
	,cpe.attribute_set_id
	,cpe.type_id
	,cpe.sku
	,cpe.has_options
	,cpe.required_options
	,cpe.created_at
	,cpe.updated_at
	,cpe.row_id
	,cpe.created_in
	,cpe.updated_in
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_catalog_product_entity cpe



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_entity()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_catalog_product_entity (
		entity_id
		,attribute_set_id
		,type_id
		,sku
		,has_options
		,required_options
		,created_at
		,updated_at
		,row_id
		,created_in
		,updated_in
		,"load_ts"
	)

	VALUES (
		NEW.entity_id
		,NEW.attribute_set_id
		,NEW.type_id
		,NEW.sku
		,NEW.has_options
		,NEW.required_options
		,NEW.created_at
		,NEW.updated_at
		,NEW.row_id
		,NEW.created_in
		,NEW.updated_in
		,CURRENT_TIMESTAMP
	)
	ON CONFLICT ("entity_id") DO UPDATE
	SET
		"entity_id" = EXCLUDED."entity_id"
		,"attribute_set_id" = EXCLUDED."attribute_set_id"
		,"type_id" = EXCLUDED."type_id"
		,"sku" = EXCLUDED."sku"
		,"has_options" = EXCLUDED."has_options"
		,"required_options" = EXCLUDED."required_options"
		,"created_at" = EXCLUDED."created_at"
		,"updated_at" = EXCLUDED."updated_at"
		,"row_id" = EXCLUDED."row_id"
		,"created_in" = EXCLUDED."created_in"
		,"updated_in" = EXCLUDED."updated_in"
		,"load_ts" = CURRENT_TIMESTAMP;

	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD CSTOMER ENTITY TABLE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_catalog_product_entity ON bronze.magento_prod_catalog_product_entity;
CREATE TRIGGER trg_upsert_magento_prod_catalog_product_entity
AFTER INSERT OR UPDATE ON bronze.magento_prod_catalog_product_entity
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_entity();

