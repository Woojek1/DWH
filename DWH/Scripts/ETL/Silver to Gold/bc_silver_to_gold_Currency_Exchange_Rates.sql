CREATE OR REPLACE VIEW gold.v_bc_currency_exchange_rates AS (
	SELECT 
	cer."Currency_Code"
	,cer."Starting_Date"
	,cer."Relational_Currency_Code"
	,cer."Exchange_Rate_Amount"
	,cer."Relational_Exch_Rate_Amount"
	,cer."Adjustment_Exch_Rate_Amount"
	,cer."Relational_Adjmt_Exch_Rate_Amt"
	,cer."EDN_Sales_Exch_Rate"
	,cer."Fix_Exchange_Rate_Amount"
	,cer."ITI_Exch_Rate_Table_No" 
	,cer."load_ts"
	FROM
		silver.bc_currency_exchange_rates cer
)