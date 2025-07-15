create or replace view gold.v_BC_adjusted_costs as

with AdjustedCostsAircon as (
SELECT 
	"Document_No"
	,concat('A_', "Document_No") as "KeyNoInvoice"
	,"Document_Line_No"
	,sum("Cost_Posted_to_G_L") * (-1) as kosztskorygowany
FROM
	bronze."bc_values_entries_aircon"
where
	"Document_Type" in ('Sales Invoice', 'Sales Credit Memo')
group by
	"Document_No"
	,"Document_Line_No"
),

AdjustedCostsTechnab as (
SELECT 
	"Document_No"
	,concat('T_', "Document_No") as "KeyNoInvoice"
	,"Document_Line_No"
	,sum("Cost_Posted_to_G_L") * (-1) as kosztskorygowany
FROM
	bronze."bc_values_entries_technab"
where
	"Document_Type" in ('Sales Invoice', 'Sales Credit Memo')
group by
	"Document_No"
	,"Document_Line_No"
),

AdjustedCostsZymetric as (
SELECT
	"Document_No"
	,concat('Z_', "Document_No") as "KeyNoInvoice"
	,"Document_Line_No"
	,sum("Cost_Posted_to_G_L") * (-1) as kosztskorygowany
FROM
	bronze."bc_values_entries_zymetric"
where
	"Document_Type" in ('Sales Invoice', 'Sales Credit Memo')
group by
	"Document_No"
	,"Document_Line_No"
)

select *
from
	AdjustedCostsAircon
union all
select *
from
	AdjustedCostsTechnab
union all
select *
from
	AdjustedCostsZymetric
	