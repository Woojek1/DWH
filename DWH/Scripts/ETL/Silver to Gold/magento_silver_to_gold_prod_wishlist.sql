CREATE OR REPLACE VIEW gold.v_magento_prod_wishlist AS
SELECT
	wishlist_id
	,customer_id
	,shared
	,sharing_code
	,updated_at
	,name
	,visibility
	,"load_ts"
FROM
	silver.magento_prod_wishlist
