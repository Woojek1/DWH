CREATE OR REPLACE VIEW gold.v_magento_prod_customer_log AS
SELECT
	"log_id"
	,"customer_id"
	,"last_login_at"
	,"last_logout_at"
	,"load_ts"
FROM
	silver.magento_prod_customer_log