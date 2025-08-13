create or replace view gold."Fact_Items_Failure" as 

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.bc_service_ledger_entry_aircon sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'

union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.bc_service_ledger_entry_technab sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'
	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.bc_service_ledger_entry_zymetric sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'
	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.nav_service_ledger_entry_aircon sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'
	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.nav_service_ledger_entry_technab sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'
	
union all

SELECT
	sle."Posting_Date" as "Failure_Date"
	,si."Posting_Date" as "Sale_Date"
	,round(
		(sle."Posting_Date" - si."Posting_Date")/30.44,
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
FROM
	silver.nav_service_ledger_entry_zymetric sle
LEFT join 
	gold."Dim_BC_NAV_Sold_Items" si
on
	sle."Item_No_Serviced" = si."Item_No"
and
	sle."Serial_No_Serviced" = si."Serial_No"
where
	sle."Entry_Type" = 'Consume'