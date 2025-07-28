select 
	inv."NIP"
	,max(inv."KeyNoCustomer") as "KeyNoCustomer"
	,max(inv."CustomerName") as "CustomerName"
	,max(inv."NoItem") as "NoItem"
	,max(inv."ItemDescription") as "ItemDescription"
	,max(inv."PostingDate") as "PostingDate"
	,max(cust."E_Mail") as "E_Mail"
	,max(cust."Phone_No") as "Phone_No"
from
	gold.v_bc_posted_sales_invoices inv
inner join
	gold.v_bc_customers cust
on inv."KeyNoCustomer" = cust."KeyNoCustomer"
where
	inv."NoItem" in ('MI/AGBPW-09NXD0-IH'
						,'MI/X1BPW-09N8D0-OH'
						,'MI/AGBPW-12NXD0-IH'
						,'MI/X1BPW-12N8D0-OH'
						,'MI/AGBPW-18NXD0-IH'
						,'MI/X3BPW-18N8D0-OH'
						,'MI/AGBPW-24NXD0-IH'
						,'MI/X4BPW-24N8D0-OH'
						)
group by
	inv."NIP"

