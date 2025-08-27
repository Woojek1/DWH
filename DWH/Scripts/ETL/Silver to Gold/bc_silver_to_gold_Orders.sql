

CREATE OR REPLACE VIEW gold.v_orders AS
WITH Orders_Aircon AS (
	SELECT
		sl."Document_No" AS "No_Order"
		,sl."Key_Document_No" AS "Key_No_Order"
		,sl."Document_Type" as "Document_Type"
		,sl."Shortcut_Dimension2_Code" AS "No_Project"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "Key_No_Project"
		,oh."Quote_No" as "No_Quote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Key_No_Quote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."Line_No" AS "OrderLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"		
		,sl."Line_Amount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN"
		,sl."Cooling_Capacity_KW" AS "CoolingCapacity"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "CustomerName"
		,oh."Salesperson_Code" AS "SalespersonCode"
		,oh."Status" AS "Status"
		,oh."OrderStatus" AS "OrderStatus"
		,GREATEST(oh."load_ts", sl."load_ts") as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_sales_lines_aircon sl
	INNER JOIN
		silver.bc_sales_orders_header_aircon oh
	ON 
		sl."Document_No" = oh."No"
),

Orders_Technab AS (
	SELECT
		sl."Document_No" AS "No_Order"
		,sl."Key_Document_No" AS "Key_No_Order"
		,sl."Document_Type" as "Document_Type"
		,sl."Shortcut_Dimension2_Code" AS "No_Project"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "Key_No_Project"
		,oh."Quote_No" as "No_Quote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Key_No_Quote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."Line_No" AS "OrderLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"		
		,sl."Line_Amount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN"
		,sl."Cooling_Capacity_KW" AS "CoolingCapacity"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "CustomerName"
		,oh."Salesperson_Code" AS "SalespersonCode"
		,oh."Status" AS "Status"
		,oh."OrderStatus" AS "OrderStatus"
		,GREATEST(oh."load_ts", sl."load_ts") as "LoadDate"
		,'Technab' AS "Company"
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_orders_header_technab oh
	ON 
		sl."Document_No" = oh."No"
	),
	
Orders_Zymetric AS (
	SELECT
		sl."Document_No" AS "No_Order"
		,sl."Key_Document_No" AS "Key_No_Order"
		,sl."Document_Type" as "Document_Type"
		,sl."Shortcut_Dimension2_Code" AS "No_Project"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "Key_No_Project"
		,oh."Quote_No" as "No_Quote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Key_No_Quote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."Line_No" AS "OrderLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"		
		,sl."Line_Amount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * sl."Price_List_Exchange_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN"
		,sl."Cooling_Capacity_KW" AS "CoolingCapacity"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "CustomerName"
		,oh."Salesperson_Code" AS "SalespersonCode"
		,oh."Status" AS "Status"
		,oh."OrderStatus" AS "OrderStatus"
		,GREATEST(oh."load_ts", sl."load_ts") as "LoadDate"
		,'Zymetric' AS "Company"
	FROM
		silver.bc_sales_lines_zymetric sl
	INNER JOIN
		silver.bc_sales_orders_header_zymetric oh
	ON 
		sl."Document_No" = oh."No"
)

SELECT *
	FROM Orders_Aircon
UNION ALL
SELECT *
	FROM Orders_Technab
UNION ALL
SELECT *
	FROM Orders_Zymetric
;