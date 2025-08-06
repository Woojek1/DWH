CREATE OR REPLACE VIEW gold.v_magento_prod_wishlist_item AS
SELECT
	wishlist_item_id
	,wishlist_id
	,product_id
	,store_id
	,added_at
	,description
	,qty
	,"load_ts"
FROM
	silver.magento_prod_wishlist_item
