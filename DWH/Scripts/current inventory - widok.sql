SELECT
	item_no,
	sum(cost_amount_actual),
	sum(available_quantity)
FROM
	gold."Fact_Current_Inventory"
WHERE
	item_no = 'MD/MOX13312HFN8QRD1N'
and
	company = 'A'
group by
	item_no


select * from gold."Fact_Reservation"