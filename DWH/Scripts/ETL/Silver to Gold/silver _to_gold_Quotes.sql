CREATE OR REPLACE VIEW gold.v_oferty AS
WITH Quotes_Aircon AS (
	SELECT
		sl."documentNo" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoQuote"
		,sl."documentType" AS "documentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."lineNo" AS "QuoteLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"
		,sl."lineAmount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "NoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "CustomerName"
--		,qh."VAT_Registration_No" AS "NIP"
		,qh."Salesperson_Code" AS "SalespersonCode"
		,qh."Status" AS "QuoteStatus"
		,qh."EDN_Quote_Type_Code" AS "QuoteTypeCode"
		,GREATEST(qh."load_ts", sl."load_ts") as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_sales_lines_aircon sl
	INNER JOIN
		silver.bc_sales_quotes_header_aircon qh
	ON 
		sl."documentNo" = qh."No"
),

Quotes_Technab AS (
		SELECT
		sl."documentNo" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoQuote"
		,sl."documentType" AS "documentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."lineNo" AS "QuoteLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"
		,sl."lineAmount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "NoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "CustomerName"
--		,qh."VAT_Registration_No" AS "NIP"
		,qh."Salesperson_Code" AS "SalespersonCode"
		,qh."Status" AS "QuoteStatus"
		,qh."EDN_Quote_Type_Code" AS "QuoteTypeCode"
		,GREATEST(qh."load_ts", sl."load_ts") as "LoadDate"
		,'Technab' AS "Company"
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_quotes_header_technab qh
	ON 
		sl."documentNo" = qh."No"
),

Quotes_Zymetric AS (
	SELECT
		sl."documentNo" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "KeyNoQuote"
		,sl."documentType" AS "documentType"
		,sl."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."lineNo" AS "QuoteLine"
		,sl."no" AS "NoItem"
		,sl."quantity" AS "Quantity"
		,sl."lineAmount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."ednPriceListExchangeRate" AS "CurrencyExchangeRate (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "LineAmountPLN"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "ProfitPLN"
		,sl."ednCoolingCapacityKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "NoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "CustomerName"
--		,qh."VAT_Registration_No" AS "NIP"
		,qh."Salesperson_Code" AS "SalespersonCode"
		,qh."Status" AS "QuoteStatus"
		,qh."EDN_Quote_Type_Code" AS "QuoteTypeCode"
		,GREATEST(qh."load_ts", sl."load_ts") as "LoadDate"
		,'Zymetric' AS "Company"
	FROM
		silver.bc_sales_lines_zymetric sl
	INNER JOIN
		silver.bc_sales_quotes_header_zymetric qh
	ON 
		sl."documentNo" = qh."No"
)

SELECT *
	FROM Quotes_Aircon
UNION ALL
SELECT *
	FROM Quotes_Technab
UNION ALL
SELECT *
	FROM Quotes_Zymetric
;


