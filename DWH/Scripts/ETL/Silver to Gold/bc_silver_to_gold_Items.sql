CREATE OR REPLACE VIEW gold.v_bc_items AS
WITH Items_Aircon AS (
	SELECT 
		i."No" AS "NoItem"
		,"Key_Item_No" AS "KeyNoItem"
		,max(i."No_2") AS "Indeks PIM"
		,max(i."Inventory") AS "StockQuantity"
		,max(i."Mk_GL_Quantity") AS "MKGLStockQuantity"
		,max(i."Unit_Cost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."Base_Unit_Of_Measure")  AS "UnitOfMeasure"
		,max(i."Base_Group") AS "InventoryGroup"
		,max(i."Edn_Type_A") AS "ItemTypeA"
		,max(i."Edn_Type_B") AS "ItemTypeB"
		,max(i."Costing_Method") AS "ValuationMethod"
		,max(i."Description") AS "Description"
		,max(i."Description_2") AS "Description2"
		,max(i."Edn_Cooling_Capacity_KW") AS "CoolingCapacity"
		,max(i."Edn_Heating_Capacity_KW") AS "HeatingCapacity" 
		,max(i."Edn_Efficiency_Index") AS "PerformanceIndex"
		,max(i."Edn_Factor_Type") AS "Refrigerant"
		,max(i."Edn_Refrigerant_Quantity_UoM") AS "RefrigerantQuantity"
		,max(i."Gen_Prod_Posting_Group") AS "MainInventoryAccountingGroup"
		,max(i."Inventory_Posting_Group") AS "InventoryPostingGroup"
		,max(i."Manufacturer_Code") AS "ManufacturerCode"
		,max(i."Qty_On_Purch_Order") AS "PurchaseOrderQuantity"
		,max(i."Qty_On_Sales_Order") AS "SalesOrderQuantity"
		,max(i."Qty_On_Service_Order") AS "ServiceOrderQuantity"	
		,max(i."Reserved_Qty_On_Inventory") AS "ReservedInventoryQuantity"
		,max(i."Reserved_Qty_On_Purch_Orders") AS "ReservedPurchaseOrderQuantity"
		,max(i."Reserved_Qty_On_Sales_Orders") AS "ReservedSalesOrderQuantity"
		,i."Blocked" AS "Blocked"
		,max(i."System_Id") AS "SystemID"
		,max(i."Net_Weight") as "NetWeight"
		,max(i."Edn_Battery_Code") as "EdnBatteryCode"
		,i."Edn_Battery_Item" as "EdnBatteryItem"
		,max(i."Edn_Battery_Quantity") as "EdnBatteryQuantity"
		,max(i."Vendor_No") as "VendorNo"
		,max(i."System_Modified_At") as "SystemModifiedAt"
		,max(i."load_ts") AS "LoadDate" 
		,'Aircon' AS "Company"
	FROM
		silver.bc_items_aircon i
	left join
		silver.bc_price_lists_aircon pl
	on
		i."No" = pl."AssetNo"
	and
		pl."PriceListCode" = 'S00001'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(
		pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE
		)
	where
		i."Inventory_Posting_Group" <> 'USŁUGI'
	group by
		i."No"
	
	
),
	
Items_Technab AS (
	SELECT 
		i."No" AS "NoItem"
		,"Key_Item_No" AS "KeyNoItem"
		,max(i."No_2") AS "Indeks PIM"
		,max(i."Inventory") AS "StockQuantity"
		,max(i."Mk_GL_Quantity") AS "MKGLStockQuantity"
		,max(i."Unit_Cost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."Base_Unit_Of_Measure")  AS "UnitOfMeasure"
		,max(i."Base_Group") AS "InventoryGroup"
		,max(i."Edn_Type_A") AS "ItemTypeA"
		,max(i."Edn_Type_B") AS "ItemTypeB"
		,max(i."Costing_Method") AS "ValuationMethod"
		,max(i."Description") AS "Description"
		,max(i."Description_2") AS "Description2"
		,max(i."Edn_Cooling_Capacity_KW") AS "CoolingCapacity"
		,max(i."Edn_Heating_Capacity_KW") AS "HeatingCapacity" 
		,max(i."Edn_Efficiency_Index") AS "PerformanceIndex"
		,max(i."Edn_Factor_Type") AS "Refrigerant"
		,max(i."Edn_Refrigerant_Quantity_UoM") AS "RefrigerantQuantity"
		,max(i."Gen_Prod_Posting_Group") AS "MainInventoryAccountingGroup"
		,max(i."Inventory_Posting_Group") AS "InventoryPostingGroup"
		,max(i."Manufacturer_Code") AS "ManufacturerCode"
		,max(i."Qty_On_Purch_Order") AS "PurchaseOrderQuantity"
		,max(i."Qty_On_Sales_Order") AS "SalesOrderQuantity"
		,max(i."Qty_On_Service_Order") AS "ServiceOrderQuantity"	
		,max(i."Reserved_Qty_On_Inventory") AS "ReservedInventoryQuantity"
		,max(i."Reserved_Qty_On_Purch_Orders") AS "ReservedPurchaseOrderQuantity"
		,max(i."Reserved_Qty_On_Sales_Orders") AS "ReservedSalesOrderQuantity"
		,i."Blocked" AS "Blocked"
		,max(i."System_Id") AS "SystemID"
		,max(i."Net_Weight") as "NetWeight"
		,max(i."Edn_Battery_Code") as "EdnBatteryCode"
		,i."Edn_Battery_Item" as "EdnBatteryItem"
		,max(i."Edn_Battery_Quantity") as "EdnBatteryQuantity"
		,max(i."Vendor_No") as "VendorNo"
		,max(i."System_Modified_At") as "SystemModifiedAt"
		,max(i."load_ts") AS "LoadDate" 
		,'Technab' AS "Company"
	FROM
		silver.bc_items_technab i
	left join
		silver.bc_price_lists_technab pl
	on
		i."No" = pl."AssetNo"
	and
		pl."PriceListCode" = 'S00001'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(
		pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE
		)
	where
		i."Inventory_Posting_Group" <> 'USŁUGI'
	group by
		i."No"
),
	
Items_Zymetric AS (
	SELECT 
		i."No" AS "NoItem"
		,"Key_Item_No" AS "KeyNoItem"
		,max(i."No_2") AS "Indeks PIM"
		,max(i."Inventory") AS "StockQuantity"
		,max(i."Mk_GL_Quantity") AS "MKGLStockQuantity"
		,max(i."Unit_Cost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."Base_Unit_Of_Measure")  AS "UnitOfMeasure"
		,max(i."Base_Group") AS "InventoryGroup"
		,max(i."Edn_Type_A") AS "ItemTypeA"
		,max(i."Edn_Type_B") AS "ItemTypeB"
		,max(i."Costing_Method") AS "ValuationMethod"
		,max(i."Description") AS "Description"
		,max(i."Description_2") AS "Description2"
		,max(i."Edn_Cooling_Capacity_KW") AS "CoolingCapacity"
		,max(i."Edn_Heating_Capacity_KW") AS "HeatingCapacity" 
		,max(i."Edn_Efficiency_Index") AS "PerformanceIndex"
		,max(i."Edn_Factor_Type") AS "Refrigerant"
		,max(i."Edn_Refrigerant_Quantity_UoM") AS "RefrigerantQuantity"
		,max(i."Gen_Prod_Posting_Group") AS "MainInventoryAccountingGroup"
		,max(i."Inventory_Posting_Group") AS "InventoryPostingGroup"
		,max(i."Manufacturer_Code") AS "ManufacturerCode"
		,max(i."Qty_On_Purch_Order") AS "PurchaseOrderQuantity"
		,max(i."Qty_On_Sales_Order") AS "SalesOrderQuantity"
		,max(i."Qty_On_Service_Order") AS "ServiceOrderQuantity"	
		,max(i."Reserved_Qty_On_Inventory") AS "ReservedInventoryQuantity"
		,max(i."Reserved_Qty_On_Purch_Orders") AS "ReservedPurchaseOrderQuantity"
		,max(i."Reserved_Qty_On_Sales_Orders") AS "ReservedSalesOrderQuantity"
		,i."Blocked" AS "Blocked"
		,max(i."System_Id") AS "SystemID"
		,max(i."Net_Weight") as "NetWeight"
		,max(i."Edn_Battery_Code") as "EdnBatteryCode"
		,i."Edn_Battery_Item" as "EdnBatteryItem"
		,max(i."Edn_Battery_Quantity") as "EdnBatteryQuantity"
		,max(i."Vendor_No") as "VendorNo"
		,max(i."System_Modified_At") as "SystemModifiedAt"
		,max(i."load_ts") AS "LoadDate" 
		,'Zymetric' AS "Company" 
	FROM
		silver.bc_items_zymetric i
	left join
		silver.bc_price_lists_zymetric pl
	on
		i."No" = pl."AssetNo"
	and
		pl."PriceListCode" = 'S00001'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(
		pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE
		)
	where
		i."Inventory_Posting_Group" <> 'USŁUGI'
	group by
		i."No"
)
	
SELECT *
FROM
	Items_Aircon
UNION ALL
SELECT *
FROM
	Items_Technab
UNION ALL
SELECT *
FROM
	Items_Zymetric
	
;