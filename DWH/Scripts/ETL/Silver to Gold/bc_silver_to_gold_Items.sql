CREATE OR REPLACE VIEW gold.v_bc_items AS
WITH Items_Aircon AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."mkGLQuantity" AS "MKGLStockQuantity"
		,i."unitCost" AS "UnitCost"
		,pl."UnitPricePLN" AS "UnitPricePLN"
		,i."baseUnitOfMeasure"  AS "UnitOfMeasure"
		,i."baseGroup" AS "InventoryGroup"
		,i."ednTypeA" AS "ItemTypeA"
		,i."ednTypeB" AS "ItemTypeB"
		,i."costingMethod" AS "ValuationMethod"
		,i."description" AS "Description"
		,i."description2" AS "Description2"
		,i."ednCoolingCapacityKW" AS "CoolingCapacity"
		,i."ednHeatingCapacityKW" AS "HeatingCapacity" 
		,i."ednEfficiencyIndex" AS "PerformanceIndex"
		,i."ednFactorType" AS "Refrigerant"
		,i."ednRefrigerantQuantityUoM" AS "RefrigerantQuantity"
		,i."genProdPostingGroup" AS "MainInventoryAccountingGroup"
		,i."inventoryPostingGroup" AS "InventoryPostingGroup"
		,i."manufacturerCode" AS "ManufacturerCode"
		,i."qtyOnPurchOrder" AS "PurchaseOrderQuantity"
		,i."qtyOnSalesOrder" AS "SalesOrderQuantity"
		,i."qtyOnServiceOrder" AS "ServiceOrderQuantity"	
		,i."reservedQtyOnInventory" AS "ReservedInventoryQuantity"
		,i."reservedQtyOnPurchOrders" AS "ReservedPurchaseOrderQuantity"
		,i."reservedQtyOnSalesOrders" AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,i."systemId" AS "SystemID"
		,i."netWeight"
		,i."ednBatteryCode"
		,i."ednBatteryItem"
		,i."ednBatteryQuantity"
		,i."vendorNo"
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate" 
		,'Aircon' AS "Company"
	FROM
		silver.bc_items_aircon i
	left join
		SILVER.bc_price_lists_aircon pl
	on
		i."No" = pl."AssetNo"
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
	and
		pl."PriceListCode" = 'S00001'
	
	
),
	
Items_Technab AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."mkGLQuantity" AS "MKGLStockQuantity"
		,i."unitCost" AS "UnitCost"
		,pl."UnitPricePLN" AS "UnitPricePLN"
		,i."baseUnitOfMeasure"  AS "UnitOfMeasure"
		,i."baseGroup" AS "InventoryGroup"
		,i."ednTypeA" AS "ItemTypeA"
		,i."ednTypeB" AS "ItemTypeB"
		,i."costingMethod" AS "ValuationMethod"
		,i."description" AS "Description"
		,i."description2" AS "Description2"
		,i."ednCoolingCapacityKW" AS "CoolingCapacity"
		,i."ednHeatingCapacityKW" AS "HeatingCapacity" 
		,i."ednEfficiencyIndex" AS "PerformanceIndex"
		,i."ednFactorType" AS "Refrigerant"
		,i."ednRefrigerantQuantityUoM" AS "RefrigerantQuantity"
		,i."genProdPostingGroup" AS "MainInventoryAccountingGroup"
		,i."inventoryPostingGroup" AS "InventoryPostingGroup"
		,i."manufacturerCode" AS "ManufacturerCode"
		,i."qtyOnPurchOrder" AS "PurchaseOrderQuantity"
		,i."qtyOnSalesOrder" AS "SalesOrderQuantity"
		,i."qtyOnServiceOrder" AS "ServiceOrderQuantity"	
		,i."reservedQtyOnInventory" AS "ReservedInventoryQuantity"
		,i."reservedQtyOnPurchOrders" AS "ReservedPurchaseOrderQuantity"
		,i."reservedQtyOnSalesOrders" AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,i."systemId" AS "SystemID"
		,i."netWeight"
		,i."ednBatteryCode"
		,i."ednBatteryItem"
		,i."ednBatteryQuantity"
		,i."vendorNo"
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate" 
		,'Technab' AS "Firma"
	FROM
		silver.bc_items_technab i
	left join
		silver.bc_price_lists_technab pl
	on
		i."No" = pl."AssetNo"
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
	and
		pl."PriceListCode" = 'S00001'
),
	
Items_Zymetric AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."mkGLQuantity" AS "MKGLStockQuantity"
		,i."unitCost" AS "UnitCost"
		,pl."UnitPricePLN" AS "UnitPricePLN"		
		,i."baseUnitOfMeasure"  AS "UnitOfMeasure"
		,i."baseGroup" AS "InventoryGroup"
		,i."ednTypeA" AS "ItemTypeA"
		,i."ednTypeB" AS "ItemTypeB"
		,i."costingMethod" AS "ValuationMethod"
		,i."description" AS "Description"
		,i."description2" AS "Description2"
		,i."ednCoolingCapacityKW" AS "CoolingCapacity"
		,i."ednHeatingCapacityKW" AS "HeatingCapacity" 
		,i."ednEfficiencyIndex" AS "PerformanceIndex"
		,i."ednFactorType" AS "Refrigerant"
		,i."ednRefrigerantQuantityUoM" AS "RefrigerantQuantity"
		,i."genProdPostingGroup" AS "MainInventoryAccountingGroup"
		,i."inventoryPostingGroup" AS "InventoryPostingGroup"
		,i."manufacturerCode" AS "ManufacturerCode"
		,i."qtyOnPurchOrder" AS "PurchaseOrderQuantity"
		,i."qtyOnSalesOrder" AS "SalesOrderQuantity"
		,i."qtyOnServiceOrder" AS "ServiceOrderQuantity"	
		,i."reservedQtyOnInventory" AS "ReservedInventoryQuantity"
		,i."reservedQtyOnPurchOrders" AS "ReservedPurchaseOrderQuantity"
		,i."reservedQtyOnSalesOrders" AS "ReservedSalesOrderQuantity"
		,i."blocked" AS "Blocked"
		,i."systemId" AS "SystemID"
		,i."netWeight"
		,i."ednBatteryCode"
		,i."ednBatteryItem"
		,i."ednBatteryQuantity"
		,i."vendorNo"
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_items_zymetric i
	left join
		silver.bc_price_lists_zymetric pl
	on
		i."No" = pl."AssetNo"
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
	and
		pl."StartingDate" < CURRENT_DATE
	and
		(pl."EndingDate" = '0001-01-01' or pl."EndingDate" > CURRENT_DATE)
	and
		pl."PriceListCode" = 'S00001'
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