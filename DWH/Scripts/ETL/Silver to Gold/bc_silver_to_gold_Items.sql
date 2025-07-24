CREATE OR REPLACE VIEW gold.v_bc_items AS
WITH Items_Aircon AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,max(i."no2") AS "Indeks PIM"
		,max(i."inventory") AS "StockQuantity"
		,max(i."mkGLQuantity") AS "MKGLStockQuantity"
		,max(i."unitCost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."baseUnitOfMeasure")  AS "UnitOfMeasure"
		,max(i."baseGroup") AS "InventoryGroup"
		,max(i."ednTypeA") AS "ItemTypeA"
		,max(i."ednTypeB") AS "ItemTypeB"
		,max(i."costingMethod") AS "ValuationMethod"
		,max(i."description") AS "Description"
		,max(i."description2") AS "Description2"
		,max(i."ednCoolingCapacityKW") AS "CoolingCapacity"
		,max(i."ednHeatingCapacityKW") AS "HeatingCapacity" 
		,max(i."ednEfficiencyIndex") AS "PerformanceIndex"
		,max(i."ednFactorType") AS "Refrigerant"
		,max(i."ednRefrigerantQuantityUoM") AS "RefrigerantQuantity"
		,max(i."genProdPostingGroup") AS "MainInventoryAccountingGroup"
		,max(i."inventoryPostingGroup") AS "InventoryPostingGroup"
		,max(i."manufacturerCode") AS "ManufacturerCode"
		,max(i."qtyOnPurchOrder") AS "PurchaseOrderQuantity"
		,max(i."qtyOnSalesOrder") AS "SalesOrderQuantity"
		,max(i."qtyOnServiceOrder") AS "ServiceOrderQuantity"	
		,max(i."reservedQtyOnInventory") AS "ReservedInventoryQuantity"
		,max(i."reservedQtyOnPurchOrders") AS "ReservedPurchaseOrderQuantity"
		,max(i."reservedQtyOnSalesOrders") AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,max(i."systemId") AS "SystemID"
		,max(i."netWeight") as "NetWeight"
		,max(i."ednBatteryCode") as "EdnBatteryCode"
		,i."ednBatteryItem" as "EdnBatteryItem"
		,max(i."ednBatteryQuantity") as "EdnBatteryQuantity"
		,max(i."vendorNo") as "VendorNo"
		,max(i."systemModifiedAt") as "SystemModifiedAt"
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
		i."inventoryPostingGroup" <> 'USŁUGI'
	group by
		i."No"
	
	
),
	
Items_Technab AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,max(i."no2") AS "Indeks PIM"
		,max(i."inventory") AS "StockQuantity"
		,max(i."mkGLQuantity") AS "MKGLStockQuantity"
		,max(i."unitCost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."baseUnitOfMeasure")  AS "UnitOfMeasure"
		,max(i."baseGroup") AS "InventoryGroup"
		,max(i."ednTypeA") AS "ItemTypeA"
		,max(i."ednTypeB") AS "ItemTypeB"
		,max(i."costingMethod") AS "ValuationMethod"
		,max(i."description") AS "Description"
		,max(i."description2") AS "Description2"
		,max(i."ednCoolingCapacityKW") AS "CoolingCapacity"
		,max(i."ednHeatingCapacityKW") AS "HeatingCapacity" 
		,max(i."ednEfficiencyIndex") AS "PerformanceIndex"
		,max(i."ednFactorType") AS "Refrigerant"
		,max(i."ednRefrigerantQuantityUoM") AS "RefrigerantQuantity"
		,max(i."genProdPostingGroup") AS "MainInventoryAccountingGroup"
		,max(i."inventoryPostingGroup") AS "InventoryPostingGroup"
		,max(i."manufacturerCode") AS "ManufacturerCode"
		,max(i."qtyOnPurchOrder") AS "PurchaseOrderQuantity"
		,max(i."qtyOnSalesOrder") AS "SalesOrderQuantity"
		,max(i."qtyOnServiceOrder") AS "ServiceOrderQuantity"	
		,max(i."reservedQtyOnInventory") AS "ReservedInventoryQuantity"
		,max(i."reservedQtyOnPurchOrders") AS "ReservedPurchaseOrderQuantity"
		,max(i."reservedQtyOnSalesOrders") AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,max(i."systemId") AS "SystemID"
		,max(i."netWeight") as "NetWeight"
		,max(i."ednBatteryCode") as "EdnBatteryCode"
		,i."ednBatteryItem" as "EdnBatteryItem"
		,max(i."ednBatteryQuantity") as "EdnBatteryQuantity"
		,max(i."vendorNo") as "VendorNo"
		,max(i."systemModifiedAt") as "SystemModifiedAt"
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
		i."inventoryPostingGroup" <> 'USŁUGI'
	group by
		i."No"
),
	
Items_Zymetric AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,max(i."no2") AS "Indeks PIM"
		,max(i."inventory") AS "StockQuantity"
		,max(i."mkGLQuantity") AS "MKGLStockQuantity"
		,max(i."unitCost") AS "UnitCost"
		,max(pl."UnitPricePLN") AS "UnitPricePLN"
		,sum(case when pl."CurrencyCode" = 'CZK' then pl."UnitPrice" else 0 end) as "UnitPriceCZK"
		,sum(case when pl."CurrencyCode" = 'USD' then pl."UnitPrice" else 0 end) as "UnitPriceUSD"
		,sum(case when pl."CurrencyCode" = 'EUR' then pl."UnitPrice" else 0 end) as"UnitPriceEUR"
		,sum(case when pl."CurrencyCode" = 'GBP' then pl."UnitPrice" else 0 end) as "UnitPriceGBP"
		,max(i."baseUnitOfMeasure")  AS "UnitOfMeasure"
		,max(i."baseGroup") AS "InventoryGroup"
		,max(i."ednTypeA") AS "ItemTypeA"
		,max(i."ednTypeB") AS "ItemTypeB"
		,max(i."costingMethod") AS "ValuationMethod"
		,max(i."description") AS "Description"
		,max(i."description2") AS "Description2"
		,max(i."ednCoolingCapacityKW") AS "CoolingCapacity"
		,max(i."ednHeatingCapacityKW") AS "HeatingCapacity" 
		,max(i."ednEfficiencyIndex") AS "PerformanceIndex"
		,max(i."ednFactorType") AS "Refrigerant"
		,max(i."ednRefrigerantQuantityUoM") AS "RefrigerantQuantity"
		,max(i."genProdPostingGroup") AS "MainInventoryAccountingGroup"
		,max(i."inventoryPostingGroup") AS "InventoryPostingGroup"
		,max(i."manufacturerCode") AS "ManufacturerCode"
		,max(i."qtyOnPurchOrder") AS "PurchaseOrderQuantity"
		,max(i."qtyOnSalesOrder") AS "SalesOrderQuantity"
		,max(i."qtyOnServiceOrder") AS "ServiceOrderQuantity"	
		,max(i."reservedQtyOnInventory") AS "ReservedInventoryQuantity"
		,max(i."reservedQtyOnPurchOrders") AS "ReservedPurchaseOrderQuantity"
		,max(i."reservedQtyOnSalesOrders") AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,max(i."systemId") AS "SystemID"
		,max(i."netWeight") as "NetWeight"
		,max(i."ednBatteryCode") as "EdnBatteryCode"
		,i."ednBatteryItem" as "EdnBatteryItem"
		,max(i."ednBatteryQuantity") as "EdnBatteryQuantity"
		,max(i."vendorNo") as "VendorNo"
		,max(i."systemModifiedAt") as "SystemModifiedAt"
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
		i."inventoryPostingGroup" <> 'USŁUGI'
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