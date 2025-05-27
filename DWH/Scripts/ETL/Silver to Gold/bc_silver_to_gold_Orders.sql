select * from silver.bc_sales_lines_zymetric

-------------------------------------------------

CREATE OR REPLACE VIEW gold.v_orders AS
WITH Orders_Aircon AS (
	SELECT
		sl."documentNo" AS "NoOrder"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoOrder"
		,sl."documentType" as "DocumentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,oh."Quote_No" as "NoQuote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "KeyNoQuote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."lineNo" AS "OrderLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"		
		,sl."lineAmount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
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
		sl."documentNo" = oh."No"
),

Orders_Technab AS (
	SELECT
		sl."documentNo" AS "NoOrder"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoOrder"
		,sl."documentType" as "DocumentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,oh."Quote_No" as "NoQuote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "KeyNoQuote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."lineNo" AS "OrderLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"		
		,sl."lineAmount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "CustomerName"
		,oh."Salesperson_Code" AS "SalespersonCode"
		,oh."Status" AS "Status"
		,oh."OrderStatus" AS "OrderStatus"
		,GREATEST(oh."load_ts", sl."load_ts") as "LoadDate"
		,'Technab' AS "Firma"
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_orders_header_technab oh
	ON 
		sl."documentNo" = oh."No"
	),
	
Orders_Zymetric AS (
	SELECT
		sl."documentNo" AS "NoOrder"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoOrder"
		,sl."documentType" as "DocumentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,oh."Quote_No" as "NoQuote"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "KeyNoQuote"
		,oh."Posting_Date" AS "OrderDate"
		,sl."lineNo" AS "OrderLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"		
		,sl."lineAmount" AS "LineAmount"
		,oh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate" -- do zmiany
		,CASE
			WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN oh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "CustomerName"
		,oh."Salesperson_Code" AS "SalespersonCode"
		,oh."Status" AS "Status"
		,oh."OrderStatus" AS "OrderStatus"
		,GREATEST(oh."load_ts", sl."load_ts") as "LoadDate"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_sales_lines_zymetric sl
	INNER JOIN
		silver.bc_sales_orders_header_zymetric oh
	ON 
		sl."documentNo" = oh."No"
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