-----------------------------------------------------------------------------
-- TWORZENIE TABELI W WARSTWIE SILVER
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.magento_prod_catalog_product_bundle_option (
    option_id int8 NOT NULL PRIMARY KEY,
    parent_id int8 NOT NULL,
    required int2 DEFAULT 0 NOT NULL,
    "position" int4 DEFAULT 0 NOT NULL,
    "type" varchar(32) NOT NULL,
    load_ts timestamptz NULL
);

-----------------------------------------------------------------------------
-- PIERWSZE ŁADOWANIE DANYCH Z BRONZE DO SILVER
-----------------------------------------------------------------------------

INSERT INTO silver.magento_prod_catalog_product_bundle_option (
    option_id,
    parent_id,
    required,
    "position",
    "type",
    load_ts
)
SELECT
    bo.option_id,
    bo.parent_id,
    bo.required,
    bo."position",
    bo."type",
    CURRENT_TIMESTAMP
FROM bronze.magento_prod_catalog_product_bundle_option bo;

-----------------------------------------------------------------------------
-- FUNKCJA UPSERT Z BRONZE DO SILVER
-----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_bundle_option()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO silver.magento_prod_catalog_product_bundle_option (
        option_id,
        parent_id,
        required,
        "position",
        "type",
        load_ts
    )
    VALUES (
        NEW.option_id,
        NEW.parent_id,
        NEW.required,
        NEW."position",
        NEW."type",
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (option_id) DO UPDATE
    SET
        parent_id = EXCLUDED.parent_id,
        required  = EXCLUDED.required,
        "position" = EXCLUDED."position",
        "type"     = EXCLUDED."type",
        load_ts    = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$;

-----------------------------------------------------------------------------
-- TRIGGER W WARSTWIE BRONZE
-----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_upsert_magento_prod_catalog_product_bundle_option
ON bronze.magento_prod_catalog_product_bundle_option;

CREATE TRIGGER trg_upsert_magento_prod_catalog_product_bundle_option
AFTER INSERT OR UPDATE ON bronze.magento_prod_catalog_product_bundle_option
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_bundle_option();
