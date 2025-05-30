CREATE OR REPLACE VIEW gold.v_bc_items AS
WITH Items_Aircon AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."unitCost" AS "UnitCost"
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
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate" 
		,'Aircon' AS "Company"
	FROM
		silver.bc_items_aircon i
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
),
	
Items_Technab AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."unitCost" AS "UnitCost"
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
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate" 
		,'Technab' AS "Firma"
	FROM
		silver.bc_items_technab i
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
),
	
Items_Zymetric AS (
	SELECT 
		i."No" AS "NoItem"
		,CONCAT(i."Firma", '_', "No") AS "KeyNoItem"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "StockQuantity"
		,i."unitCost" AS "UnitCost"
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
		,i."systemModifiedAt" as "SystemModifiedAt"
		,i."load_ts" AS "LoadDate"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_items_zymetric i
	WHERE 
		i."inventoryPostingGroup" <> 'USŁUGI'
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