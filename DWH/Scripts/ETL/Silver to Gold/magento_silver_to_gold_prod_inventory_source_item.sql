CREATE OR REPLACE VIEW gold.v_magento_prod_inventory_source_item AS
SELECT
	*
FROM 
	silver.magento_prod_inventory_source_item