with
Faktury as (
	SELECT
		"NoInvoice" as "NoInvoice"
		,max("PostingDate") as "PostingDate"
		,round(sum("AmountLCY"),2) as "AmountLCY"
		,round(sum("Koszt skorygowany"),2) as "Koszt skorygowany"
		,"Company"
	FROM
		gold.v_bc_posted_sales_invoices
	where 
		"Type" = 'Item'
	and
		"Company" = 'Aircon'
	group by
		"NoInvoice", "Company"
),

Korekty as (
	SELECT
		"NoInvoice" as "NoInvoice"
		,max("PostingDate") as "PostingDate"
		,round(sum("AmountLCY"),2) as "AmountLCY"
		,round(sum("Koszt skorygowany"),2) as "Koszt skorygowany"
		,"Company"
	FROM
		gold.v_bc_posted_sales_credit_memo_invoices
	where
		"Type" in ('Charge (Item)', 'Item')
	and
		"Company" = 'Aircon'
	group by
		"NoInvoice", "Company"
),

Faktury_i_korekty as (
	select *
	from Faktury
	union all
	select *
	from Korekty 
),


Value_Entry as (
	select
		"Document_No" as "Document_No"
		,round(sum("Sales_Amount_Actual"),2) as "ValueEntry_Sales_Amount_Actual"
		,round(sum("Cost_Posted_to_G_L") * (-1),2) as "ValueEntry_Cost_Posted_to_G_L"
		,max('Aircon') as "Company"
	from
		bronze.bc_values_entries_aircon
	where
		"Document_Type" in ('Sales Invoice', 'Sales Credit Memo')
	group by
		"Document_No"
)

select *
from 
	Faktury_i_korekty fik
right join
	Value_Entry ve
on
	fik."NoInvoice" = ve."Document_No"
--and 
--	fik."Company" = ve."Company"
--where fik."Company" = 'Aircon'





		