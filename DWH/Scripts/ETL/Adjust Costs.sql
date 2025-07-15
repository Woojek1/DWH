create view gold.AdjustedCosts as
SELECT 
	"Document No_"
	,"Document Line No_"
	,sum("Cost Posted to G_L") * (-1) as kosztskorygowany
FROM
	bronze."Aircon$Value Entry$437dbf0e-84ff-417a-965d-ed2bb9650972"
group by
	"Document No_"
	,"Document Line No_"

