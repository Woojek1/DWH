create or replace view gold."Fact_Items_Failure" as 

with sold_items as (

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
	),
	
last_sales as (
	
	select DISTINCT ON 
		
		("Item_No"
		,"Serial_No")
	    "Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Item_No"
		,"Serial_No"
		,"Firma"
		,count("Item_No") over (partition by "Item_No") as "Total_Sold"
	FROM sold_items
	ORDER BY 
		"Item_No"
		,"Serial_No"
		,"Entry_No" desc
		,"Posting_Date" desc
	)


SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.bc_service_ledger_entry_aircon sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 0

union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.bc_service_ledger_entry_technab sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 1

	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.bc_service_ledger_entry_zymetric sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 1

	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.nav_service_ledger_entry_aircon sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 1
	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.nav_service_ledger_entry_technab sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 1

	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,ls."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - ls."Posting_Date")/30.44,
		2
	)as "Time_To_Failure (month)"
	,sle."Service_Order_No"
	,sle."Bill_to_Customer_No"
	,sle."Service_Item_No_Serviced"
	,sle."Item_No_Serviced"
	,sle."Serial_No_Serviced"
	,sle."Base_Group_A_Serviced"
	,sle."Base_Group_B_Serviced"
	,sle."No"
	,sle."Base_GroupA"
	,sle."Base_GroupB"
	,sle."Firma"
	,ls."Total_Sold"
FROM
	silver.nav_service_ledger_entry_zymetric sle
LEFT join 
	last_sales ls
on
	sle."Item_No_Serviced" = ls."Item_No"
and
	sle."Serial_No_Serviced" = ls."Serial_No"
where
	sle."Entry_Type" = 'Consume'
and
	sle."Serial_No_Serviced" <> ''
and 
	(sle."Posting_Date" - ls."Posting_Date")/30.44 > 1

