-----------------------------------------------------------------------------
-- TWORZENIE TABELI W WARSTWIE SILVER
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.magento_prod_catalog_product_entity_varchar (
    value_id int8 NOT NULL PRIMARY KEY,
    attribute_id int4 NOT NULL,
    store_id int2 NOT NULL,
    value text NULL,
    row_id int8 NOT NULL,
    load_ts timestamptz NULL
);

-----------------------------------------------------------------------------
-- PIERWSZE ŁADOWANIE DANYCH Z BRONZE DO SILVER
-----------------------------------------------------------------------------

INSERT INTO silver.magento_prod_catalog_product_entity_varchar (
    value_id,
    attribute_id,
    store_id,
    value,
    row_id,
    load_ts
)
SELECT
    b.value_id,
    b.attribute_id,
    b.store_id,
    b.value,
    b.row_id,
    CURRENT_TIMESTAMP
FROM bronze.magento_prod_catalog_product_entity_varchar b;

-----------------------------------------------------------------------------
-- FUNKCJA UPSERT Z BRONZE DO SILVER
-----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_entity_varchar()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO silver.magento_prod_catalog_product_entity_varchar (
        value_id,
        attribute_id,
        store_id,
        value,
        row_id,
        load_ts
    )
    VALUES (
        NEW.value_id,
        NEW.attribute_id,
        NEW.store_id,
        NEW.value,
        NEW.row_id,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (value_id) DO UPDATE
    SET
        attribute_id = EXCLUDED.attribute_id,
        store_id     = EXCLUDED.store_id,
        value        = EXCLUDED.value,
        row_id       = EXCLUDED.row_id,
        load_ts      = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$;

-----------------------------------------------------------------------------
-- TRIGGER W WARSTWIE BRONZE
-----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_upsert_magento_prod_catalog_product_entity_varchar
ON bronze.magento_prod_catalog_product_entity_varchar;

CREATE TRIGGER trg_upsert_magento_prod_catalog_product_entity_varchar
AFTER INSERT OR UPDATE ON bronze.magento_prod_catalog_product_entity_varchar
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_entity_varchar();
