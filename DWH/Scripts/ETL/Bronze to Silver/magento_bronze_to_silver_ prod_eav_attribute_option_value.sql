-----------------------------------------------------------------------------
-- TWORZENIE TABELI W WARSTWIE SILVER
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.magento_prod_eav_attribute_option_value (
    value_id int8 NOT NULL PRIMARY KEY,
    option_id int8 NOT NULL,
    store_id int2 NOT NULL,
    value text NOT NULL,
    load_ts timestamptz NULL
);

-----------------------------------------------------------------------------
-- PIERWSZE ŁADOWANIE DANYCH Z BRONZE DO SILVER
-----------------------------------------------------------------------------

INSERT INTO silver.magento_prod_eav_attribute_option_value (
    value_id,
    option_id,
    store_id,
    value,
    load_ts
)
SELECT
    ev.value_id,
    ev.option_id,
    ev.store_id,
    ev.value,
    CURRENT_TIMESTAMP
FROM bronze.magento_prod_eav_attribute_option_value ev;

-----------------------------------------------------------------------------
-- FUNKCJA UPSERT Z BRONZE DO SILVER
-----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_eav_attribute_option_value()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO silver.magento_prod_eav_attribute_option_value (
        value_id,
        option_id,
        store_id,
        value,
        load_ts
    )
    VALUES (
        NEW.value_id,
        NEW.option_id,
        NEW.store_id,
        NEW.value,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (value_id) DO UPDATE
    SET
        option_id = EXCLUDED.option_id,
        store_id  = EXCLUDED.store_id,
        value     = EXCLUDED.value,
        load_ts   = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$;

-----------------------------------------------------------------------------
-- TRIGGER W WARSTWIE BRONZE
-----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_upsert_magento_prod_eav_attribute_option_value
ON bronze.magento_prod_eav_attribute_option_value;

CREATE TRIGGER trg_upsert_magento_prod_eav_attribute_option_value
AFTER INSERT OR UPDATE ON bronze.magento_prod_eav_attribute_option_value
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_eav_attribute_option_value();
