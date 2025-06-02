CREATE OR REPLACE VIEW v_bc_price_lists AS
WITH Price_Lists_Aircon AS (
	SELECT
		pl."AssetNo"
		,pl."UnitPrice"
		,pl."PriceListCode"
		,pl."StartingDate"
		,pl."EndingDate"
		,i."mkGLQuantity"
		,pl."load_ts" AS "LoadDate"
		,'Aircon' AS "Company"
		
	FROM	
		silver.bc_price_lists_aircom pl
	left join
		bronze.bc_items_aircon i
	on pl."AssetNo" = i."No"
	where
		pl."StartingDate" < CURRENT_DATE
	and
		pl."EndingDate" > CURRENT_DATE


)


-------------------
select 
"No",
"mkGLQuantity"
from
bronze.bc_items_zymetric