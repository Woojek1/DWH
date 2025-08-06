CREATE OR REPLACE VIEW gold.v_magento_prod_wishlist_item_option AS
SELECT
	option_id
	,wishlist_item_id
	,product_id
	,code
	,value
	,"load_ts"
FROM
	silver.magento_prod_wishlist_item_option
