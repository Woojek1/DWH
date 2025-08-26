WITH base AS (
         SELECT a_1."Item_No" AS item_no,
            a_1."Location_Code" AS location_code,
            a_1."Firma" AS company,
            a_1."Sales_Amount_Actual" AS sales_amount_actual,
            a_1."Cost_Amount_Actual" AS cost_amount_actual,
            a_1."Quantity" AS quantity,
            a_1."Remaining_Quantity" AS remaining_quantity,
            a_1."Reserved_Quantity" AS reserved_quantity,
            a_1."Completely_Invoiced",
            a_1.load_ts
           FROM silver.bc_items_ledger_entries_aircon a_1
        UNION ALL
         SELECT t."Item_No",
            t."Location_Code",
            t."Firma",
            t."Sales_Amount_Actual",
            t."Cost_Amount_Actual",
            t."Quantity",
            t."Remaining_Quantity",
            t."Reserved_Quantity",
            t."Completely_Invoiced",
            t.load_ts
           FROM silver.bc_items_ledger_entries_technab t
        UNION ALL
         SELECT z."Item_No",
            z."Location_Code",
            z."Firma",
            z."Sales_Amount_Actual",
            z."Cost_Amount_Actual",
            z."Quantity",
            z."Remaining_Quantity",
            z."Reserved_Quantity",
            z."Completely_Invoiced",
            z.load_ts
           FROM silver.bc_items_ledger_entries_zymetric z
        )

 select *
 from
 	base
 where
 	item_no = 'MD/MOX13312HFN8QRD1N'
order by 4,5
--and
--	sales_amount_actual = cost_amount_actual
    
SELECT 
	item_no,
	sum(cost_amount_actual),
	sum("quantity") as quantity,
	count("Completely_Invoiced")
FROM
	base
WHERE
	item_no = 'MD/MOX13312HFN8QRD1N'
and
	company = 'A'
group by
	item_no
        
	
	
        agg AS (
         SELECT base.item_no,
            base.location_code,
            base.company,
            base.sales_amount_actual,
            base.cost_amount_actual,
            sum(base.quantity) AS available_quantity,
            sum(base.remaining_quantity) AS remaining_quantity,
            sum(base.reserved_quantity) AS reserved_quantity
          FROM base
          GROUP BY 
          	base.item_no,
          	base.location_code,
          	base.company, 
          	base.sales_amount_actual,
          	base.cost_amount_actual
        )
      
  SELECT 
	item_no,
	sum(cost_amount_actual),
	sum("available_quantity") as quantity
--	count("Completely_Invoiced")
FROM
	agg
WHERE
	item_no = 'MD/MOX13312HFN8QRD1N'
and
	company = 'A'
group by
	item_no
        
        
        max_ts AS (
         SELECT max(base.load_ts) AS max_load_ts
           FROM base
        ),
final as (        
select
 	a.item_no,
    a.location_code,
    a.company,
    a.sales_amount_actual,
    a.cost_amount_actual,
    a.available_quantity,
    a.remaining_quantity,
    a.reserved_quantity
FROM agg a
where 
   	item_no = 'MD/MOX13312HFN8QRD1N'
)
   	
SELECT 
	item_no,
	company,
	location_code,
	SUM(sales_amount_actual) as sales_amount_actual,
	SUM(cost_amount_actual) as cost_amount_actual,
	SUM(available_quantity) as available_quantity,
	SUM(remaining_quantity) as remaining_quantity,
	SUM(reserved_quantity) as reserved_quantity
from
	gold."Fact_Current_Inventory"
where 
   	item_no = 'MD/MOX13312HFN8QRD1N'
group by
	item_no,
	company,
	location_code
order by
	1,2,3
	
   	
  