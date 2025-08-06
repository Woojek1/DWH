CREATE OR REPLACE VIEW gold.v_magento_prod_requisition_list_item AS
SELECT
	item_id
	,requisition_list_id
	,sku
	,store_id
	,added_at
	,qty
	,"options"
	,load_ts
FROM 
	silver.magento_prod_requisition_list_item