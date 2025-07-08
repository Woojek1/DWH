CREATE OR REPLACE VIEW gold.v_magento_prod_newsletter_subscriber AS
SELECT
	"subscriber_id"
	,"store_id"
	,"change_status_at"
	,"customer_id"
	,"subscriber_email" 
	,"subscriber_status"
	,"subscriber_confirm_code"
	,"load_ts"
FROM
	silver.magento_prod_newsletter_subscriber
	