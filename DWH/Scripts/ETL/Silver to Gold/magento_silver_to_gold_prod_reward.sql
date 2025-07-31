CREATE OR REPLACE VIEW gold.v_magento_prod_reward AS
SELECT
	reward_id
	,customer_id
	,website_id
	,points_balance
	,website_currency_code
	,load_ts
FROM 
	silver.magento_prod_reward