-----------------------------------------------------------------------------
-- TWORZENIE TABELI W WARSTWIE SILVER
-----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.magento_prod_catalog_product_bundle_selection (
    selection_id int8 NOT NULL PRIMARY KEY,
    option_id int8 NOT NULL,
    parent_product_id int8 NOT NULL,
    product_id int8 NOT NULL,
    "position" int4 NOT NULL,
    is_default int2 DEFAULT 0 NOT NULL,
    selection_price_type int2 DEFAULT 0 NOT NULL,
    selection_price_value numeric(12, 4) NOT NULL,
    selection_qty numeric(12, 4) NOT NULL,
    selection_can_change_qty int2 DEFAULT 0 NOT NULL,
    load_ts timestamptz NULL
);

-----------------------------------------------------------------------------
-- PIERWSZE ŁADOWANIE DANYCH Z BRONZE DO SILVER
-----------------------------------------------------------------------------

INSERT INTO silver.magento_prod_catalog_product_bundle_selection (
    selection_id,
    option_id,
    parent_product_id,
    product_id,
    "position",
    is_default,
    selection_price_type,
    selection_price_value,
    selection_qty,
    selection_can_change_qty,
    load_ts
)
SELECT
    bs.selection_id,
    bs.option_id,
    bs.parent_product_id,
    bs.product_id,
    bs."position",
    bs.is_default,
    bs.selection_price_type,
    bs.selection_price_value,
    bs.selection_qty,
    bs.selection_can_change_qty,
    CURRENT_TIMESTAMP
FROM bronze.magento_prod_catalog_product_bundle_selection bs;

-----------------------------------------------------------------------------
-- FUNKCJA UPDATER/UPSERT Z BRONZE DO SILVER
-----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_bundle_selection()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO silver.magento_prod_catalog_product_bundle_selection (
        selection_id,
        option_id,
        parent_product_id,
        product_id,
        "position",
        is_default,
        selection_price_type,
        selection_price_value,
        selection_qty,
        selection_can_change_qty,
        load_ts
    )
    VALUES (
        NEW.selection_id,
        NEW.option_id,
        NEW.parent_product_id,
        NEW.product_id,
        NEW."position",
        NEW.is_default,
        NEW.selection_price_type,
        NEW.selection_price_value,
        NEW.selection_qty,
        NEW.selection_can_change_qty,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (selection_id) DO UPDATE
    SET
        option_id              = EXCLUDED.option_id,
        parent_product_id      = EXCLUDED.parent_product_id,
        product_id             = EXCLUDED.product_id,
        "position"             = EXCLUDED."position",
        is_default             = EXCLUDED.is_default,
        selection_price_type   = EXCLUDED.selection_price_type,
        selection_price_value  = EXCLUDED.selection_price_value,
        selection_qty          = EXCLUDED.selection_qty,
        selection_can_change_qty = EXCLUDED.selection_can_change_qty,
        load_ts                = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$;

-----------------------------------------------------------------------------
-- TRIGGER W WARSTWIE BRONZE
-----------------------------------------------------------------------------

DROP TRIGGER IF EXISTS trg_upsert_magento_prod_catalog_product_bundle_selection
ON bronze.magento_prod_catalog_product_bundle_selection;

CREATE TRIGGER trg_upsert_magento_prod_catalog_product_bundle_selection
AFTER INSERT OR UPDATE ON bronze.magento_prod_catalog_product_bundle_selection
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_catalog_product_bundle_selection();
