create or replace view gold.Zapotrzebowanie_na_czesci as 

with failure_table as (

	select
		ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") as "Numer_wiersza (i)"
		,COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") AS "Ilosc_awarii (n)"
		,((ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") - 0.3)::numeric) 
			/ (COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") + 0.4)
		AS "Empiryczne prawdopodobienstwo_awarii (Ti)"	
		,ln(
			"Time_To_Failure (month)"
			) 
			as "X"		
		,ln(
			-ln(
				1-((ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") - 0.3)::numeric) 
				/ (COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") + 0.4)
					))
				as "Y"
		,"Failure_Date"
		,"Sale_Date"
		,"Time_To_Failure (month)"
		,"Item_No_Serviced"
		,"No"
		,"Base_GroupA"
		,"Base_GroupB"
	from
		gold."Fact_Items_Failure" fil
	where
		"Time_To_Failure (month)" > 1
	and
		"Item_No_Serviced" <> 'G-EF07UMX'
--	and
--		"Base_GroupB" = 'PCB_358'
	)
	
	select
		"Item_No_Serviced"
  		,"Base_GroupB"
  		,regr_slope("Y", "X") as "a (wsp. kierunkowy)"
  		,regr_intercept("Y", "X") as "b (wyraz wolny)" 
  		,EXP(- regr_intercept("Y","X") / regr_slope("Y","X")) AS eta
  		,max(sia."Quantity") as "Sold_quantity"
  		,max("Ilosc_awarii (n)") as "Ilosc_awarii"
--  		,1-exp(
--  			-power(2/(EXP(- regr_intercept("Y","X") / regr_slope("Y","X"))),
--  			regr_slope("Y", "X")))
--  		as "Prawdopodobieństwo wystąpienia awarii (6 mc)"
--  		,(1-exp(
--  			-power(2/(EXP(- regr_intercept("Y","X") / regr_slope("Y","X"))),
--  			regr_slope("Y", "X"))))
--  			*
--  		max(sia."Quantity")
--  		*
--  		max("Ilosc_awarii (n)")
--  		/
--  		max(sia."Quantity") as "Zapotrzebowanie (6mc)"		
	from
		failure_table ft
	left join
		gold."Dim_BC_NAV_Sold_Items_Aggregation" sia
	on ft."Item_No_Serviced" = sia."Item_No"
	where 
		"Empiryczne prawdopodobienstwo_awarii (Ti)"	> 0
	group by
		"Item_No_Serviced"
		,"Base_GroupB"
	having
		max("Ilosc_awarii (n)") > 1
	and
		regr_slope("Y", "X") is not null



	