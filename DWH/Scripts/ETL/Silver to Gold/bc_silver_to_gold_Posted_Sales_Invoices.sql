select
	*
from
	silver.bc_posted_sales_invoices_lines_zymetric sil
inner join
	silver.bc_posted_sales_invoices_header_zymetric sih
on sil."documentNo" = sih."No"
inner join 
	silver.bc_customers_zymetric c
on sil."sellToCustomerNo" = c."No"
inner join
	silver.bc_dimension_set_zymetric ds
on sil."dimensionSetID" = ds."dimensionSetID"

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW  gold.v_bc_posted_sales_invoices AS
WITH Invoices_Aircon AS (
	select
		sil."documentNo" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."documentNo") AS "KeyNoInvoice"
		,sil."lineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."shortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"	
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."type" as "Type"
		,sil."no" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."no") AS "KeyNoItem"		
		,sil."description2" AS "ItemDescription"
		,sil."quantity" AS "Quantity"
		,sil."amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor"))) 
			else sil."amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "LineCostsLCY"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor")))
			else sil."amount"
		end) - 
		((sil."unitCostLCY") * (sil."quantity"))) AS "ProfitLCY"
		,(sil."lineDiscount"/100) as "LineDiscount"
		,sil."lineDiscountAmount" as "LineDiscountAmount"
		,(sil."ednSalesMargin"/100) AS "MarginBC"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,MAX(sih."Salesperson_Code") as "Salesperson"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_posted_sales_invoices_lines_aircon sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_aircon sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_aircon c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_aircon ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
	),
	
Invoices_Technab AS (
		select
		sil."documentNo" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."documentNo") AS "KeyNoInvoice"
		,sil."lineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."shortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"	
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."type" as "Type"
		,sil."no" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."no") AS "KeyNoItem"		
		,sil."description2" AS "ItemDescription"
		,sil."quantity" AS "Quantity"
		,sil."amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor"))) 
			else sil."amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "LineCostsLCY"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor")))
			else sil."amount"
		end) - 
		((sil."unitCostLCY") * (sil."quantity"))) AS "ProfitLCY"
		,(sil."lineDiscount"/100) as "LineDiscount"
		,sil."lineDiscountAmount" as "LineDiscountAmount"
		,(sil."ednSalesMargin"/100) AS "MarginBC"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,MAX(sih."Salesperson_Code") as "Salesperson"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Technab' AS "Firma"
	FROM
		silver.bc_posted_sales_invoices_lines_technab sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_technab sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_technab c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_technab ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
),

Invoices_Zymetric AS (
	select
		sil."documentNo" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."documentNo") AS "KeyNoInvoice"
		,sil."lineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."shortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."shortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"	
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."type" as "Type"
		,sil."no" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."no") AS "KeyNoItem"		
		,sil."description2" AS "ItemDescription"
		,sil."quantity" AS "Quantity"
		,sil."amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor"))) 
			else sil."amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "LineCostsLCY"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amount"/(Max(sih."Currency_Factor")))
			else sil."amount"
		end) - 
		((sil."unitCostLCY") * (sil."quantity"))) AS "ProfitLCY"
		,(sil."lineDiscount"/100) as "LineDiscount"
		,sil."lineDiscountAmount" as "LineDiscountAmount"
		,(sil."ednSalesMargin"/100) AS "MarginBC"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,MAX(sih."Salesperson_Code") as "Salesperson"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_posted_sales_invoices_lines_zymetric sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_zymetric sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_zymetric c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_zymetric ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
	)
	
SELECT *
	FROM Invoices_Aircon
UNION ALL
SELECT *
	FROM Invoices_Technab
UNION ALL
SELECT *
	FROM Invoices_Zymetric
;
