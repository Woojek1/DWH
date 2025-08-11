select
	c."GenBusPostingGroup"
	,EXTRACT(YEAR FROM inv."PostingDate") AS "Year"
	,EXTRACT(QUARTER FROM inv."PostingDate") AS "Quarter"
	,EXTRACT(MONTH from inv."PostingDate") as "Month"
	,max(inv."Company") as "Company"
	,max(it."ManufacturerCode") as "ManufacturerCode"
	,inv."NoItem"
	,sum(inv."Quantity") as "Quantity"
	,sum(inv."AmountLCY") as "AmountLCY"
	,sum(inv."Koszt skorygowany") as "CostLCY"	
from
	gold.v_bc_posted_sales_invoices inv
left join
	gold.v_bc_items it
on inv."KeyNoItem" = it."KeyNoItem"
left join
	gold.v_bc_customers c
on
	inv."KeyNoCustomer" = c."KeyNoCustomer"
where
	EXTRACT(YEAR FROM inv."PostingDate") = 2025
and
	EXTRACT(MONTH FROM inv."PostingDate") <= 7
and
	inv."Company" in ('Aircon', 'Zymetric')
and
	inv."Type" = 'Towar'
and
	c."RelatedCompany" = 'Niepowiązane'
group by
	c."GenBusPostingGroup"
	,EXTRACT(YEAR FROM inv."PostingDate")
	,EXTRACT(QUARTER FROM inv."PostingDate")
	,EXTRACT(MONTH from inv."PostingDate")
	,inv."NoItem"
order by
	5 asc, 1 asc, 2 asc, 3 asc 
