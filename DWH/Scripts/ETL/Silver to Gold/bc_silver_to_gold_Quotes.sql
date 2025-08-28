CREATE OR REPLACE VIEW gold.v_bc_quotes AS
WITH Quotes_Aircon AS (
	SELECT
		sl."Document_No" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."Document_No") AS "KeyNoQuote"
		,sl."Document_Type" AS "DocumentType"
		,sl."Shortcut_Dimension2_Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."Line_No" AS "QuoteLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"
		,sl."Line_Amount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,cer."Relational_Exch_Rate_Amount" as "RelationalExchRateAmount"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * cer."Relational_Exch_Rate_Amount")
		END AS "LineAmountPLN"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN" 
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * cer."EDN_Sales_Exch_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN" 
		,sl."Price_Catalogue" as "PriceCatalogue"
		,sl."Line_Discount" as "LineDiscount"
		,sl."Line_Discount_Amount" as "LineDiscountAmount"
		,sl."Total_Cooling_CapKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
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
		sl."Document_No" = qh."No"
	left join 
		silver.bc_currency_exchange_rates cer
	on
		qh."Currency_Code" = cer."Currency_Code"
	and
		qh."Document_Date" = date(cer."Starting_Date")
),

Quotes_Technab AS (
	SELECT
		sl."Document_No" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."Document_No") AS "KeyNoQuote"
		,sl."Document_Type" AS "DocumentType"
		,sl."Shortcut_Dimension2_Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."Line_No" AS "QuoteLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"
		,sl."Line_Amount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,cer."Relational_Exch_Rate_Amount" as "RelationalExchRateAmount"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * cer."Relational_Exch_Rate_Amount")
		END AS "LineAmountPLN"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * cer."EDN_Sales_Exch_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN" 
		,sl."Price_Catalogue" as "PriceCatalogue"
		,sl."Line_Discount" as "LineDiscount"
		,sl."Line_Discount_Amount" as "LineDiscountAmount"
		,sl."Total_Cooling_CapKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
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
		sl."Document_No" = qh."No"
	left join 
		silver.bc_currency_exchange_rates cer
	on
		qh."Currency_Code" = cer."Currency_Code"
	and
		qh."Document_Date" = date(cer."Starting_Date")
),

Quotes_Zymetric AS (
	SELECT
		sl."Document_No" AS "NoQuote"
		,CONCAT(sl."Firma", '_', sl."Document_No") AS "KeyNoQuote"
		,sl."Document_Type" AS "DocumentType"
		,sl."Shortcut_Dimension2_Code" AS "NoProject"
		,CONCAT(sl."Firma", '_', sl."Shortcut_Dimension2_Code") AS "KeyNoProject"
		,qh."Document_Date" AS "QuoteDate"
		,sl."Line_No" AS "QuoteLine"
		,sl."No_Item" AS "NoItem"
		,sl."Quantity" AS "Quantity"
		,sl."Line_Amount" AS "LineAmount"
		,qh."Currency_Code" as "CurrencyCode"
		,sl."Price_List_Exchange_Rate" AS "CurrencyExchangeRate" -- do zmiany
		,cer."Relational_Exch_Rate_Amount" as "RelationalExchRateAmount"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
			ELSE (sl."Line_Amount" * cer."Relational_Exch_Rate_Amount")
		END AS "LineAmountPLN"
		,(sl."Ory_UnitCost_LCY") * (sl."Quantity") AS "LineCostsPLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."Line_Amount"
				ELSE (sl."Line_Amount" * cer."EDN_Sales_Exch_Rate")
			END
		) - (sl."Ory_UnitCost_LCY") * (sl."Quantity")
			AS "ProfitPLN" 
		,sl."Price_Catalogue" as "PriceCatalogue"
		,sl."Line_Discount" as "LineDiscount"
		,sl."Line_Discount_Amount" as "LineDiscountAmount"
		,sl."Total_Cooling_CapKW" AS "CoolingCapacity"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "KeyNoCustomer"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
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
		sl."Document_No" = qh."No"
	left join 
		silver.bc_currency_exchange_rates cer
	on
		qh."Currency_Code" = cer."Currency_Code"
	and
		qh."Document_Date" = date(cer."Starting_Date")
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


