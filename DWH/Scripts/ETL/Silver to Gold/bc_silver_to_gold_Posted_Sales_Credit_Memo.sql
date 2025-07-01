CREATE OR REPLACE VIEW  gold.v_invoices as
WITH BC_Correction_Invoices_Aircon AS (
	select
		sil."DocumentNo" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."DocumentNo") AS "KeyNoInvoice"
		,sil."lineNo" AS "InvoiceLine"	
from 
	silver.bc_posted_sales_credit_memo_lines_aircon sil
	
	
-------------------------------------------------------------------------------------------------------------------------------	
-------------------------------------------------------------------------------------------------------------------------------	
	
CREATE OR REPLACE VIEW  gold.v_bc_posted_sales_credit_memo_invoices AS
--WITH BC_Invoices_Aircon AS (
	select
		sil."Document_No" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."Document_No") AS "KeyNoInvoice"
		,sil."Line_No" AS "InvoiceLine"
		,null /*sih."Quote_No"*/ AS "NoQuote"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Quote_No")*/ AS "KeyNoQuote"		-- nie ma na fakturach korygujących
		,sil."Shortcut_Dimension_2_Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."Shortcut_Dimension_2_Code") AS "KeyNoProject"
		,null /*sih."Order_No"*/ AS "NoOrder"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Order_No")*/ AS "KeyNoOrder"		-- nie ma na fakturach korygujących
		,MAX(sih."Posting_Date") AS "PostingDate"
		,null /*MAX(sih."Due_Date")*/ as "DueDate"		-- nie ma na fakturach korygujących
		,null /*case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end */ as "DaysAfterDueDate" 		-- nie ma na fakturach korygujących
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,cer."Relational_Exch_Rate_Amount" as "CurrencyFactor"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description_2" AS "ItemDescription"
		,sil."Quantity" AS "Quantity"
		,sil."Unit_Price" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Unit_Price"*(Max(cer."Relational_Exch_Rate_Amount"))) * (sil."Quantity")
			else sil."Unit_Price" * (sil."Quantity")
		end as "LinePriceLCY"
		,(sil."Amount") * (-1) AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"*(Max(cer."Relational_Exch_Rate_Amount")) * (-1)) 
			else (sil."Amount") * (-1)
		end as "AmountLCY"		
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
--		,(sil."Unit_Cost_LCY") * (sil."Quantity") 
		,0 AS "LineCostsLCY"
--		,((case 
--			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount"*(Max(cer."Relational_Exch_Rate_Amount")) * (-1))
--			else (sil."Amount" * (-1))
--		end) - 
--		((sil."Unit_Cost_LCY") * (sil."Quantity"))) as
		,0 as"ProfitLCY"
		,(sil."Line_Discount_Percent"/100) as "LineDiscount"
		,sil."Line_Discount_Amount" as "LineDiscountAmount"
		,0 /*(sil."ednSalesMargin"/100)*/ AS "MarginBC"		-- nie ma na fakturach korygujących
--		,CASE 
--		  WHEN (sil."unitCostLCY" * sil."quantity") = 0 THEN 0
--		  ELSE 
--		    (
--		      (
--		        CASE
--		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
--		          ELSE sil."amount"
--		        END
--		      ) - (sil."unitCostLCY" * sil."quantity")
--		    ) 
--		    /
--		    NULLIF(
--		      (
--		        CASE
--		          WHEN MAX(sih."Currency_Code") IN ('EUR', 'USD') THEN sil."amount" / MAX(sih."Currency_Factor")
--		          ELSE sil."amount"
--		        END
--		      ),
--		      0
--		    )
--		END AS 
		,0 as "Profitability"
		,(sil."Amount_Including_VAT") * (-1) AS "AmountIncludingVAT"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount_Including_VAT"/(Max(cer."Relational_Exch_Rate_Amount")) * (-1)) 
			else (sil."Amount_Including_VAT") * (-1)
		end as "AmountIncludingVatLCY"		
		,0 as "RemainingAmount"
		,0 as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "SalespersonCode"
		,MAX(sp."Name") as "SalespersonName"
		,null /*MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END)*/ AS "Region"
		,sil."Shortcut_Dimension_1_Code" AS "MPK"
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_posted_sales_credit_memo_lines_zymetric sil
	INNER JOIN
		silver.bc_posted_sales_credit_memo_header_zymetric sih
	ON sil."Document_No" = sih."No"
	left JOIN 
		silver.bc_salesperson_aircon sp
	ON sih."Salesperson_Code" = sp."Code"
	left join
		silver.bc_currency_exchange_rates cer
	on
		sih."Currency_Code" = cer."Currency_Code"
	and
		sih."Document_Date" = date(cer."Starting_Date")
	GROUP BY
		sil."Document_No",
		sil."Firma",
		sil."Line_No",
--		sih."Quote_No",
--		sih."Order_No",
		sil."Shortcut_Dimension_2_Code",
		sil."Type",
		sil."No",
		sil."Description_2",
		sil."Quantity",
		sil."Unit_Price",
		sil."Amount",
		sil."Unit_Cost_LCY",
		sil."Line_Discount_Percent",
		sil."Line_Discount_Amount",
		sil."Amount_Including_VAT",
		sil."Shortcut_Dimension_1_Code",
		sih."VAT_Registration_No",
		cer."Relational_Exch_Rate_Amount"

	),