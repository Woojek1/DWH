CREATE OR REPLACE VIEW v_bc_price_lists AS
WITH Price_Lists_Aircon AS (
	select
		pl."PriceListCode"
		,pl."AssetNo"
		,pl."UnitPrice"
		,pl."StartingDate"
		,pl."EndingDate"
		,i."mkGLQuantity"
		,pl."load_ts" AS "LoadDate"
		,'Aircon' AS "Company"		
	FROM	
		silver.bc_price_lists_aircon pl
	left join
		bronze.bc_items_aircon i
	on pl."AssetNo" = i."No"
	where
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
),

Price_Lists_Technab as (
	SELECT
		pl."PriceListCode"
		,pl."AssetNo"
		,pl."UnitPrice"
		,pl."StartingDate"
		,pl."EndingDate"
		,i."mkGLQuantity"
		,pl."load_ts" AS "LoadDate"
		,'Technab' AS "Company"		
	FROM	
		silver.bc_price_lists_technab pl
	left join
		bronze.bc_items_technab i
	on pl."AssetNo" = i."No"
	where
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
),

Price_Lists_Zymetric as (
	SELECT
		pl."PriceListCode"
		,pl."AssetNo"
		,pl."UnitPrice"
		,pl."StartingDate"
		,pl."EndingDate"
		,i."mkGLQuantity"
		,pl."load_ts" AS "LoadDate"
		,'Zymetric' AS "Company"		
	FROM	
		silver.bc_price_lists_zymetric pl
	left join
		bronze.bc_items_zymetric i
	on pl."AssetNo" = i."No"
	where
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
)

select *
from
	Price_Lists_Aircon
union all

select *
from
	Price_Lists_Technab
union all

select *
from
	Price_Lists_Aircon


-------------------
select 
"No",
"mkGLQuantity"
from
bronze.bc_items_zymetric