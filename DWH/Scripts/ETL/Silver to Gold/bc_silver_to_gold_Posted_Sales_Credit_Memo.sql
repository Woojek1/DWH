CREATE OR REPLACE VIEW  gold.v_bc_posted_sales_credit_memo_invoices AS
WITH BC_Posted_Sales_Credit_Memo_Aircon AS (
	select
		sil."Document_No" AS "NoInvoice"
		,sil."Key_No_Invoice" AS "KeyNoInvoice"
		,sil."Line_No" AS "InvoiceLine"
		,null /*sih."Quote_No"*/ AS "NoQuote"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Quote_No")*/ AS "KeyNoQuote"		-- nie ma na fakturach korygujących
		,sil."Shortcut_Dimension_2_Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."Shortcut_Dimension_2_Code") AS "KeyNoProject"
		,null /*sih."Order_No"*/ AS "NoOrder"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Order_No")*/ AS "KeyNoOrder"		-- nie ma na fakturach korygujących
		,false as "ECOM"
		,MAX(sih."Posting_Date") AS "PostingDate"
		,null /*MAX(sih."Due_Date")*/ as "DueDate"		-- nie ma na fakturach korygujących
		,null /*case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end */ as "DaysAfterDueDate" 		-- nie ma na fakturach korygujących
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,max(sih."ITI_Correction_Reason") as "CorrectionReason"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description_2" AS "ItemDescription"
		,(sil."Quantity") * (-1) AS "Quantity"
		,sil."Unit_Price" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Unit_Price" / MAX(sih."Currency_Factor")) * (sil."Quantity")
			else sil."Unit_Price" * (sil."Quantity")
		end as "LinePriceLCY"
		,(sil."Amount") * (-1) AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
			else (sil."Amount") * (-1)
		end as "AmountLCY"		
	--		,sil."unitCostLCY" AS "Koszt urzadzenia"
	--		,(sil."Unit_Cost_LCY") * (sil."Quantity") 
		,(sil."Unit_Cost_LCY") * (sil."Quantity") * (-1) AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
			,((case 
				when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
				else (sil."Amount" * (-1))
			end) - 
			((sil."Unit_Cost_LCY") * (sil."Quantity") * (-1))) as "ProfitLCY (based on direct cost)"
		,(sil."Line_Discount_Percent"/100) as "LineDiscount"
		,sil."Line_Discount_Amount" as "LineDiscountAmount"
		,0 AS "MarginBC"		-- nie ma na fakturach korygujących
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
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount_Including_VAT" / (MAX(sih."Currency_Factor")) * (-1)) 
			else (sil."Amount_Including_VAT") * (-1)
		end as "AmountIncludingVatLCY"		
		,0 as "RemainingAmount"
		,0 as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "SalespersonCode"
		,MAX(sp."Name") as "SalespersonName"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."Shortcut_Dimension_1_Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"		
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_posted_sales_credit_memo_lines_aircon sil
	INNER JOIN
		silver.bc_posted_sales_credit_memo_header_aircon sih
	ON sil."Document_No" = sih."No"
	left JOIN 
		silver.bc_salesperson_aircon sp
	ON sih."Salesperson_Code" = sp."Code"
	left JOIN
		silver.bc_dimension_set_aircon ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON sil."Key_No_Invoice" = ac."KeyNoInvoice"
	and
		sil."Line_No" = ac."Document_Line_No"
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
		sih."VAT_Registration_No"

),
	
BC_Posted_Sales_Credit_Memo_Technab AS (
	select
		sil."Document_No" AS "NoInvoice"
		,sil."Key_No_Invoice" AS "KeyNoInvoice"
		,sil."Line_No" AS "InvoiceLine"
		,null /*sih."Quote_No"*/ AS "NoQuote"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Quote_No")*/ AS "KeyNoQuote"		-- nie ma na fakturach korygujących
		,sil."Shortcut_Dimension_2_Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."Shortcut_Dimension_2_Code") AS "KeyNoProject"
		,null /*sih."Order_No"*/ AS "NoOrder"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Order_No")*/ AS "KeyNoOrder"		-- nie ma na fakturach korygujących
		,false as "ECOM"
		,MAX(sih."Posting_Date") AS "PostingDate"
		,null /*MAX(sih."Due_Date")*/ as "DueDate"		-- nie ma na fakturach korygujących
		,null /*case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end */ as "DaysAfterDueDate" 		-- nie ma na fakturach korygujących
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,max(sih."ITI_Correction_Reason") as "CorrectionReason"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description_2" AS "ItemDescription"
		,(sil."Quantity") * (-1) AS "Quantity"
		,sil."Unit_Price" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Unit_Price" / MAX(sih."Currency_Factor")) * (sil."Quantity")
			else sil."Unit_Price" * (sil."Quantity")
		end as "LinePriceLCY"
		,(sil."Amount") * (-1) AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
			else (sil."Amount") * (-1)
		end as "AmountLCY"		
	--		,sil."unitCostLCY" AS "Koszt urzadzenia"
	--		,(sil."Unit_Cost_LCY") * (sil."Quantity") 
		,(sil."Unit_Cost_LCY") * (sil."Quantity") * (-1) AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
			,((case 
				when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
				else (sil."Amount" * (-1))
			end) - 
			((sil."Unit_Cost_LCY") * (sil."Quantity") * (-1))) as "ProfitLCY (based on direct cost)"
		,(sil."Line_Discount_Percent"/100) as "LineDiscount"
		,sil."Line_Discount_Amount" as "LineDiscountAmount"
		,0 AS "MarginBC"		-- nie ma na fakturach korygujących
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
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount_Including_VAT" / (MAX(sih."Currency_Factor")) * (-1)) 
			else (sil."Amount_Including_VAT") * (-1)
		end as "AmountIncludingVatLCY"		
		,0 as "RemainingAmount"
		,0 as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "SalespersonCode"
		,MAX(sp."Name") as "SalespersonName"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."Shortcut_Dimension_1_Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"		
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Technab' AS "Company"
	FROM
		silver.bc_posted_sales_credit_memo_lines_technab sil
	INNER JOIN
		silver.bc_posted_sales_credit_memo_header_technab sih
	ON sil."Document_No" = sih."No"
	left JOIN 
		silver.bc_salesperson_technab sp
	ON sih."Salesperson_Code" = sp."Code"
	left JOIN
		silver.bc_dimension_set_technab ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON sil."Key_No_Invoice" = ac."KeyNoInvoice"
	and
		sil."Line_No" = ac."Document_Line_No"
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
		sih."VAT_Registration_No"
),
	
BC_Posted_Sales_Credit_Memo_Zymetric AS (
	select
		sil."Document_No" AS "NoInvoice"
		,sil."Key_No_Invoice" AS "KeyNoInvoice"
		,sil."Line_No" AS "InvoiceLine"
		,null /*sih."Quote_No"*/ AS "NoQuote"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Quote_No")*/ AS "KeyNoQuote"		-- nie ma na fakturach korygujących
		,sil."Shortcut_Dimension_2_Code" AS "NoProject"
		,CONCAT(sil."Firma", '_', sil."Shortcut_Dimension_2_Code") AS "KeyNoProject"
		,null /*sih."Order_No"*/ AS "NoOrder"		-- nie ma na fakturach korygujących
		,null /*CONCAT(sil."Firma", '_', sih."Order_No")*/ AS "KeyNoOrder"		-- nie ma na fakturach korygujących
		,false as "ECOM"
		,MAX(sih."Posting_Date") AS "PostingDate"
		,null /*MAX(sih."Due_Date")*/ as "DueDate"		-- nie ma na fakturach korygujących
		,null /*case
			when max(sih."Remaining_Amount") > 0 and MAX(sih."Due_Date") < CURRENT_DATE then CURRENT_DATE - MAX(sih."Due_Date") end */ as "DaysAfterDueDate" 		-- nie ma na fakturach korygujących
		,MAX(sih."Sell_to_Customer_No") as "NoCustomer"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "KeyNoCustomer"
		,MAX(sih."Sell_to_Customer_Name") AS "CustomerName"
		,sih."VAT_Registration_No" AS "NIP"
		,MAX(sih."Currency_Code") as "CurrencyCode"
		,MAX(sih."Currency_Factor") as "CurrencyFactor"
		,max(sih."ITI_Correction_Reason") as "CorrectionReason"
		,sil."Type" as "Type"
		,sil."No" AS "NoItem"
		,CONCAT(sil."Firma", '_', sil."No") AS "KeyNoItem"		
		,sil."Description_2" AS "ItemDescription"
		,(sil."Quantity") * (-1) AS "Quantity"
		,sil."Unit_Price" as "UnitPrice"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Unit_Price" / MAX(sih."Currency_Factor")) * (sil."Quantity")
			else sil."Unit_Price" * (sil."Quantity")
		end as "LinePriceLCY"
		,(sil."Amount") * (-1) AS "Amount"
		,case 
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
			else (sil."Amount") * (-1)
		end as "AmountLCY"		
	--		,sil."unitCostLCY" AS "Koszt urzadzenia"
	--		,(sil."Unit_Cost_LCY") * (sil."Quantity") 
		,(sil."Unit_Cost_LCY") * (sil."Quantity") * (-1) AS "LineCostsLCY"
		,max(ac."kosztskorygowany")  AS "Koszt skorygowany"
			,((case 
				when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount" / (MAX(sih."Currency_Factor")) * (-1))
				else (sil."Amount" * (-1))
			end) - 
			((sil."Unit_Cost_LCY") * (sil."Quantity") * (-1))) as "ProfitLCY (based on direct cost)"
		,(sil."Line_Discount_Percent"/100) as "LineDiscount"
		,sil."Line_Discount_Amount" as "LineDiscountAmount"
		,0 AS "MarginBC"		-- nie ma na fakturach korygujących
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
			when MAX(sih."Currency_Code") in ('EUR', 'USD') then (sil."Amount_Including_VAT" / (MAX(sih."Currency_Factor")) * (-1)) 
			else (sil."Amount_Including_VAT") * (-1)
		end as "AmountIncludingVatLCY"		
		,0 as "RemainingAmount"
		,0 as "RemainingAmountLCY"
		,MAX(sih."Payment_Method_Code") AS "PaymentMethodCode"
		,MAX(sih."Salesperson_Code") as "SalespersonCode"
		,MAX(sp."Name") as "SalespersonName"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."Shortcut_Dimension_1_Code" AS "MPK"
		,max(sih."Invoice_Type") as "Invoice_Type"		
		,MAX(GREATEST(sih."load_ts", sil."load_ts")) as "LoadDate"
		,'Zymetric' AS "Company"
	FROM
		silver.bc_posted_sales_credit_memo_lines_zymetric sil
	INNER JOIN
		silver.bc_posted_sales_credit_memo_header_zymetric sih
	ON sil."Document_No" = sih."No"
	left JOIN 
		silver.bc_salesperson_zymetric sp
	ON sih."Salesperson_Code" = sp."Code"
	left JOIN
		silver.bc_dimension_set_zymetric ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	left JOIN
		gold."v_bc_adjusted_costs" ac
	ON sil."Key_No_Invoice" = ac."KeyNoInvoice"
	and
		sil."Line_No" = ac."Document_Line_No"
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
		sih."VAT_Registration_No"
	)
	
	
select * 
from
	BC_Posted_Sales_Credit_Memo_Aircon
union all
select *
from
	BC_Posted_Sales_Credit_Memo_Technab
union all
select *
from
	BC_Posted_Sales_Credit_Memo_Zymetric
		
