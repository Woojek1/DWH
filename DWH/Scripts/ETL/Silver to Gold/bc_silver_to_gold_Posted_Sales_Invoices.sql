CREATE OR REPLACE VIEW  gold.v_bc_posted_sales_invoices AS
WITH BC_Invoices_Aircon AS (
	select
		sil."DocumentNo" AS "NoInvoice"
		,sil."KeyNoInvoice" AS "KeyNoInvoice"
		,sil."LineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."ShortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."ShortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "ECOM"		
		,sil."PostingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description2" AS "ItemDescription"
		,sil."Quantity" AS "Quantity"
		,sil."UnitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."UnitPrice"/(Max(sih."Currency_Factor"))) * (sil."Quantity")
			else sil."UnitPrice" * (sil."Quantity")
		end as "LinePriceLCY"	
		,sil."Amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor"))) 
			else sil."Amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."EdnOryUnitCostLCY") * (sil."Quantity") AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor")))
			else sil."Amount"
		end) - 
		((sil."UnitCostLCY") * (sil."Quantity"))) AS "ProfitLCY (based on direct cost)"
		,(sil."LineDiscount"/100) as "LineDiscount"
		,sil."LineDiscountAmount" as "LineDiscountAmount"
		,(sil."EdnSalesMargin"/100) AS "MarginBC"
		,CASE 
		  WHEN (sil."UnitCostLCY" * sil."Quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ) - (sil."UnitCostLCY" * sil."Quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."AmountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."AmountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."AmountIncludingVAT"
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
		,sil."ShortcutDimension1Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_posted_sales_invoices_lines_aircon sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_aircon sih
	ON sil."DocumentNo" = sih."No"
	INNER JOIN 
		silver.bc_salesperson_aircon sp
	ON sih."Salesperson_Code" = sp."Code"
	INNER JOIN
		silver.bc_dimension_set_aircon ds
	ON sil."DimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON  sil."Key_No_Invoice" = ac."KeyNoInvoice"
	and
		sil."LineNo" = ac."Document_Line_No"
	GROUP BY
		sil."DocumentNo"
		,sil."LineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."PostingDate"
		,sih."VAT_Registration_No"
		,sil."Amount"
		,sil."UnitCostLCY"
		,sil."EdnSalesMargin"
		,sil."AmountIncludingVAT"
		,sil."No"
		,sil."Description2"
		,sil."ShortcutDimension1Code"
		,sil."ShortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."PostingDate" DESC
		,sil."DocumentNo" ASC
		,sil."No" ASC
	),
	
BC_Invoices_Technab AS (
	select
		sil."DocumentNo" AS "NoInvoice"
		,sil."KeyNoInvoice" AS "KeyNoInvoice"
		,sil."LineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."ShortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."ShortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "ECOM"		
		,sil."PostingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description2" AS "ItemDescription"
		,sil."Quantity" AS "Quantity"
		,sil."UnitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."UnitPrice"/(Max(sih."Currency_Factor"))) * (sil."Quantity")
			else sil."UnitPrice" * (sil."Quantity")
		end as "LinePriceLCY"	
		,sil."Amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor"))) 
			else sil."Amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."EdnOryUnitCostLCY") * (sil."Quantity") AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor")))
			else sil."Amount"
		end) - 
		((sil."UnitCostLCY") * (sil."Quantity"))) AS "ProfitLCY (based on direct cost)"
		,(sil."LineDiscount"/100) as "LineDiscount"
		,sil."LineDiscountAmount" as "LineDiscountAmount"
		,(sil."EdnSalesMargin"/100) AS "MarginBC"
		,CASE 
		  WHEN (sil."UnitCostLCY" * sil."Quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ) - (sil."UnitCostLCY" * sil."Quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."AmountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."AmountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."AmountIncludingVAT"
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
		,sil."ShortcutDimension1Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Technab' AS "Company"
	FROM
		silver.bc_posted_sales_invoices_lines_technab sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_technab sih
	ON sil."DocumentNo" = sih."No"
	INNER JOIN 
		silver.bc_salesperson_technab sp
	ON sih."Salesperson_Code" = sp."Code"
	INNER JOIN
		silver.bc_dimension_set_technab ds
	ON sil."DimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON  sil."KeyNoInvoice" = ac."KeyNoInvoice"
	and
		sil."LineNo" = ac."Document_Line_No"
	GROUP BY
		sil."DocumentNo"
		,sil."LineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."PostingDate"
		,sih."VAT_Registration_No"
		,sil."Amount"
		,sil."UnitCostLCY"
		,sil."EdnSalesMargin"
		,sil."AmountIncludingVAT"
		,sil."No"
		,sil."Description2"
		,sil."ShortcutDimension1Code"
		,sil."ShortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."PostingDate" DESC
		,sil."DocumentNo" ASC
		,sil."No" ASC
),

BC_Invoices_Zymetric AS (
	select
		sil."DocumentNo" AS "NoInvoice"
		,sil."KeyNoInvoice" AS "KeyNoInvoice"
		,sil."LineNo" AS "InvoiceLine"
		,sih."Quote_No" AS "NoQuote"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "KeyNoQuote"
		,sil."ShortcutDimension2Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."ShortcutDimension2Code") AS "KeyNoProject"
		,sih."Order_No" AS "NoOrder"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "KeyNoOrder"
		,case
			when sih."Order_No" like '%ECM%' then true
			else false
		end as "ECOM"		
		,sil."PostingDate" AS "PostingDate"
		,MAX(sih."Due_Date") as "DueDate"
		,case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end as "DaysAfterDueDate"
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description2" AS "ItemDescription"
		,sil."Quantity" AS "Quantity"
		,sil."UnitPrice" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."UnitPrice"/(Max(sih."Currency_Factor"))) * (sil."Quantity")
			else sil."UnitPrice" * (sil."Quantity")
		end as "LinePriceLCY"	
		,sil."Amount" AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor"))) 
			else sil."Amount"
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."EdnOryUnitCostLCY") * (sil."Quantity") AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
		,((case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"/(Max(sih."Currency_Factor")))
			else sil."Amount"
		end) - 
		((sil."UnitCostLCY") * (sil."Quantity"))) AS "ProfitLCY (based on direct cost)"
		,(sil."LineDiscount"/100) as "LineDiscount"
		,sil."LineDiscountAmount" as "LineDiscountAmount"
		,(sil."EdnSalesMargin"/100) AS "MarginBC"
		,CASE 
		  WHEN (sil."UnitCostLCY" * sil."Quantity") = 0 THEN 0
		  ELSE 
		    (
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ) - (sil."UnitCostLCY" * sil."Quantity")
		    ) 
		    /
		    NULLIF(
		      (
		        CASE
		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."Amount" / MAX(sih."Currency_Factor")
		          ELSE sil."Amount"
		        END
		      ),
		      0
		    )
		END AS "Profitability"
		,sil."AmountIncludingVAT" AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."AmountIncludingVAT"/(Max(sih."Currency_Factor"))) 
			else sil."AmountIncludingVAT"
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
		,sil."ShortcutDimension1Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Zymetric' AS "Company"
	FROM
		silver.bc_posted_sales_invoices_lines_zymetric sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_zymetric sih
	ON sil."DocumentNo" = sih."No"
	INNER JOIN 
		silver.bc_salesperson_zymetric sp
	ON sih."Salesperson_Code" = sp."Code"
	INNER JOIN
		silver.bc_dimension_set_zymetric ds
	ON sil."DimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON  sil."KeyNoInvoice" = ac."KeyNoInvoice"
	and
		sil."LineNo" = ac."Document_Line_No"
	GROUP BY
		sil."DocumentNo"
		,sil."LineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."PostingDate"
		,sih."VAT_Registration_No"
		,sil."Amount"
		,sil."UnitCostLCY"
		,sil."EdnSalesMargin"
		,sil."AmountIncludingVAT"
		,sil."No"
		,sil."Description2"
		,sil."ShortcutDimension1Code"
		,sil."ShortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."PostingDate" DESC
		,sil."DocumentNo" ASC
		,sil."No" ASC
)


SELECT *
	FROM BC_Invoices_Aircon
UNION ALL
SELECT *
	FROM BC_Invoices_Technab
UNION ALL
SELECT *
	FROM BC_Invoices_Zymetric
