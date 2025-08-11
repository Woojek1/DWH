create or replace view gold."Dim_BC_NAV_Sold_Items_Aggregation" AS

WITH sold_items AS (

	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.bc_items_ledger_entries_aircon
	where
		"Entry_Type" = 'Sale'
		
	union all
	
	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.bc_items_ledger_entries_technab
	where
		"Entry_Type" = 'Sale'
		
	union all
	
	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.bc_items_ledger_entries_zymetric
	where
		"Entry_Type" = 'Sale'
		
	union all
	
	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.nav_items_ledger_entries_aircon
	where
		"Entry_Type" = 'Sale'
		
	union all
	
	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.nav_items_ledger_entries_technab
	where
		"Entry_Type" = 'Sale'
		
	union all
	
	SELECT
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
	FROM
		silver.nav_items_ledger_entries_zymetric
	where
		"Entry_Type" = 'Sale'
)

SELECT 
	"Item_No"
	,COUNT(*) as "Quantity"
FROM 
	sold_items
GROUP BY
	"Item_No"