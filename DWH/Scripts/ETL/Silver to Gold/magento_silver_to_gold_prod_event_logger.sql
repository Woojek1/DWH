CREATE OR REPLACE VIEW gold.v_magento_prod_event_logger AS
SELECT
	event_id
	,user_id
	,event_type
	,event_value
	,event_time
	,load_ts
FROM silver.magento_prod_event_logger
