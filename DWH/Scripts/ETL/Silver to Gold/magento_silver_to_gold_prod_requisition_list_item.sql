CREATE OR REPLACE VIEW gold.v_magento_prod_requisition_list AS
SELECT
	"entity_id"
	,"customer_id"
	,"name"
	,"description"
	,"updated_at"
	,"load_ts"
FROM 
	silver.magento_prod_requisition_list