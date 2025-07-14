create view gold.AdjustedCosts as
SELECT 
	"Document No_"
	,"Item No_"
	,sum("Cost Posted to G_L") * (-1) as kosztskorygowany
	,sum("Cost Posted to G_L") / sum("Invoiced Quantity") as "skorygowany koszt na sztuke"
	,SUM
FROM
	bronze."Aircon$Value Entry$437dbf0e-84ff-417a-965d-ed2bb9650972"
--where
--	"Document No_" = 'FVS/25/06/0023'
group by
	"Document No_"
	,"Item No_"

	
	
	
SELECT 
	"Document No_"
	,"Item No_"
	,sum("Cost Posted to G_L") * (-1) as kosztskorygowany
	,sum("Cost Posted to G_L") / sum("Invoiced Quantity") as "skorygowany koszt na sztuke"
	,SUM
FROM
	bronze."Aircon$Value Entry$437dbf0e-84ff-417a-965d-ed2bb9650972"
--where
--	"Document No_" = 'FVS/25/06/0023'
group by
	"Document No_"
	,"Item No_"

