CREATE OR REPLACE VIEW gold.v_magento_prod_expanded_event_logger AS
SELECT
	event_id
	,user_id
	,session_id
	,event_type
	,event_value
	,ip_address
	,os
	,referer
	,destination
	,event_time
	,load_ts
FROM silver.magento_prod_expanded_event_logger
