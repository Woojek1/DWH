create or replace view gold."Dim_Sold_Items" as
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