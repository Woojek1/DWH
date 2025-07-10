CREATE OR REPLACE VIEW  gold.v_bc_posted_sales_invoices AS
WITH BC_Invoices_Aircon AS (
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
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "ECOM"		
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
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
		,sil."unitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."unitPrice"/(Max(sih."Currency_Factor"))) * (sil."quantity")
			else sil."unitPrice" * (sil."quantity")
		end as "LinePriceLCY"	
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
		,CASE 
		  WHEN (sil."unitCostLCY" * sil."quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ) - (sil."unitCostLCY" * sil."quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."amountIncludingVAT"
		end as "AmountIncludingVatLCY"		
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (Max(sih."Remaining_Amount")/(Max(sih."Currency_Factor"))) 
			else MAX(sih."Remaining_Amount")
		end as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "SalespersonCode"
		,MAX(sp."Name") as "SalespersonName"
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
		silver.bc_salesperson_aircon sp
	ON sih."Salesperson_Code" = sp."Code"
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
	
BC_Invoices_Technab AS (
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
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "ECOM"		
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
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
		,sil."unitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."unitPrice"/(Max(sih."Currency_Factor"))) * (sil."quantity")
			else sil."unitPrice" * (sil."quantity")
		end as "LinePriceLCY"	
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
		,CASE 
		  WHEN (sil."unitCostLCY" * sil."quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ) - (sil."unitCostLCY" * sil."quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."amountIncludingVAT"
		end as "AmountIncludingVatLCY"		
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (Max(sih."Remaining_Amount")/(Max(sih."Currency_Factor"))) 
			else MAX(sih."Remaining_Amount")
		end as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "Salesperson"
		,MAX(sp."Name") as "SalespersonName"
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
		silver.bc_salesperson_technab sp
	ON sih."Salesperson_Code" = sp."Code"
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

BC_Invoices_Zymetric AS (
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
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "Ecom"		
		,sil."postingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
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
		,sil."unitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."unitPrice"/(Max(sih."Currency_Factor"))) * (sil."quantity")
			else sil."unitPrice" * (sil."quantity")
		end as "LinePriceLCY"	
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
		,CASE 
		  WHEN (sil."unitCostLCY" * sil."quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ) - (sil."unitCostLCY" * sil."quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
		          ELSE sil."amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."amountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."amountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."amountIncludingVAT"
		end as "AmountIncludingVatLCY"		
		,max(sih."Remaining_Amount") as "RemainingAmount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (Max(sih."Remaining_Amount")/(Max(sih."Currency_Factor"))) 
			else MAX(sih."Remaining_Amount")
		end as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "Salesperson"
		,MAX(sp."Name") as "SalespersonName"
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
		silver.bc_salesperson_zymetric sp
	ON sih."Salesperson_Code" = sp."Code"
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
	FROM BC_Invoices_Aircon
UNION ALL
SELECT *
	FROM BC_Invoices_Technab
UNION ALL
SELECT *
	FROM BC_Invoices_Zymetric

