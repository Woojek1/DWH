create or replace view gold."Dim_BC_NAV_Sold_Items" as

with ile_union as (

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.bc_items_ledger_entries_aircon
where
	"Entry_Type" = 'Sale'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%AIRCON%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%TECHNAB%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%ZYMETRIC%'
	
union all

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.bc_items_ledger_entries_technab
where
	"Entry_Type" = 'Sale'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%AIRCON%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%TECHNAB%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%ZYMETRIC%'
	
union all

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.bc_items_ledger_entries_zymetric
where
	"Entry_Type" = 'Sale'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%AIRCON%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%TECHNAB%'
AND UPPER("EDN_Contractor_Name") NOT LIKE '%ZYMETRIC%'

union all

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.nav_items_ledger_entries_aircon
where
	"Entry_Type" = 'Sale'
and 
	"EDN_Source_No" not in ('N/01318', 'N/01056', 'N/04587', 'N/00854', 'N/02940', 'N/03595', 'N/04005')

union all

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.nav_items_ledger_entries_technab
where
	"Entry_Type" = 'Sale'
and 
	"EDN_Source_No" not in ('N/02190', 'N/04170', 'N/00009', 'N/01594', 'N/04171')

union all

SELECT
	"Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Quantity"
	,"Firma"
FROM
	silver.nav_items_ledger_entries_zymetric
where
	"Entry_Type" = 'Sale'
and 
	"EDN_Source_No" not in ('N/02599', 'N/00879', 'N/05835', 'N/06611', 'N/01591', 'N/01592', 'N/01593' , 'N/01594', 'N/01762')

)

SELECT DISTINCT ON 
	(
	"Item_No"
	,"Serial_No"
	)
    "Entry_No"
	,"Posting_Date"
	,"Entry_Type"
	,"Item_No"
	,"Serial_No"
	,"Firma"
	,count("Item_No") over (partition by "Item_No") as "Sold_Item_No_Quantity"
FROM ile_union
ORDER BY 
	"Item_No"
	,"Serial_No"
	,"Posting_Date" desc
	, "Entry_No" DESC;