-----------------------------------------------------------
-- CREATING ITEMS TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------


DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY['aircon', 'technab', 'zymetric'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_items_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text NULL
			,"Key_Item_No" text NULL
			,"Base_Unit_Of_Measure" text NULL
			,"Base_Group" text NULL
			,"Blocked" bool NULL
			,"Costing_Method" text NULL
			,"Description" text NULL
			,"Description_2" text NULL
			,"Edn_Cooling_Capacity_KW" numeric(14,2) NULL
			,"Edn_Efficiency_Index" numeric(14,2) NULL
			,"Edn_Factor_Type" text NULL
			,"Edn_Heating_Capacity_KW" numeric(14,2) NULL
			,"Edn_NAV_Key" text NULL
			,"Edn_Refrigerant" text NULL
			,"Edn_Refrigerant_Quantity_UoM" numeric(14,2) NULL
			,"Edn_Type_A" text NULL
			,"Edn_Type_B" text NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"Inventory" numeric(14,2) NULL
			,"Inventory_Posting_Group" text NULL
			,"Mk_GL_Quantity" numeric(14,2) NULL
			,"Manufacturer_Code" text NULL
			,"Maximum_Inventory" numeric(14,2) NULL
			,"Maximum_Order_Quantity" numeric(14,2) NULL
			,"Minimum_Order_Quantity" numeric(14,2) NULL
			,"Negative_Adjmt_LCY" numeric(14,2) NULL
			,"Negative_Adjmt_Qty" numeric(14,2) NULL
			,"Net_Change" numeric(14,2) NULL
			,"Net_Invoiced_Qty" numeric(14,2) NULL
			,"No_2" text NULL
			,"Qty_Assigned_To_Ship" numeric(14,2) NULL
			,"Qty_Picked" numeric(14,2) NULL
			,"Qty_In_Transit" numeric(14,2) NULL
			,"Qty_On_Asm_Component" numeric(14,2) NULL
			,"Qty_On_Assembly_Order" numeric(14,2) NULL
			,"Qty_On_Component_Lines" numeric(14,2) NULL
			,"Qty_On_Job_Order" numeric(14,2) NULL
			,"Qty_On_Prod_Order" numeric(14,2) NULL
			,"Qty_On_Purch_Order" numeric(14,2) NULL
			,"Qty_On_Purch_Return" numeric(14,2) NULL
			,"Qty_On_Sales_Order" numeric(14,2) NULL
			,"Qty_On_Sales_Return" numeric(14,2) NULL
			,"Qty_On_Service_Order" numeric(14,2) NULL
			,"Rel_Order_Receipt_Qty" numeric(14,2) NULL
			,"Reserved_Qty_On_Inventory" numeric(14,2) NULL
			,"Reserved_Qty_On_Prod_Order" numeric(14,2) NULL
			,"Reserved_Qty_On_Purch_Orders" numeric(14,2) NULL
			,"Reserved_Qty_On_Sales_Orders" numeric(14,2) NULL
			,"System_Id" text NULL
			,"System_Modified_At" timestamptz NULL
			,"Unit_Cost" numeric(14,2) NULL
			,"Net_Weight" numeric(14,2) NULL
			,"Edn_Battery_Code" text NULL
			,"Edn_Battery_Item" bool NULL
			,"Edn_Battery_Quantity" numeric(14,2) NULL
			,"Vendor_No" text NULL
			,"Alternative_Item_No" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
 			,PRIMARY KEY ("No")
		);
	$ddl$, _tabela, _litera_firmy);


 
-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Key_Item_No"
			,"Base_Unit_Of_Measure"
			,"Base_Group"
			,"Blocked"
			,"Costing_Method"
			,"Description"
			,"Description_2"
			,"Edn_Cooling_Capacity_KW"
			,"Edn_Efficiency_Index"
			,"Edn_Factor_Type"
			,"Edn_Heating_Capacity_KW"
			,"Edn_NAV_Key"
			,"Edn_Refrigerant"
			,"Edn_Refrigerant_Quantity_UoM"
			,"Edn_Type_A"
			,"Edn_Type_B"
			,"Gen_Prod_Posting_Group"
			,"Inventory"
			,"Inventory_Posting_Group"
			,"Mk_GL_Quantity"
			,"Manufacturer_Code"
			,"Maximum_Inventory"
			,"Maximum_Order_Quantity"
			,"Minimum_Order_Quantity"
			,"Negative_Adjmt_LCY"
			,"Negative_Adjmt_Qty"
			,"Net_Change"
			,"Net_Invoiced_Qty"
			,"No_2"
			,"Qty_Assigned_To_Ship"
			,"Qty_Picked"
			,"Qty_In_Transit"
			,"Qty_On_Asm_Component"
			,"Qty_On_Assembly_Order"
			,"Qty_On_Component_Lines"
			,"Qty_On_Job_Order"
			,"Qty_On_Prod_Order"
			,"Qty_On_Purch_Order"
			,"Qty_On_Purch_Return"
			,"Qty_On_Sales_Order"
			,"Qty_On_Sales_Return"
			,"Qty_On_Service_Order"
			,"Rel_Order_Receipt_Qty"
			,"Reserved_Qty_On_Inventory"
			,"Reserved_Qty_On_Prod_Order"
			,"Reserved_Qty_On_Purch_Orders"
			,"Reserved_Qty_On_Sales_Orders"
			,"System_Id"
			,"System_Modified_At"
			,"Unit_Cost"
			,"Net_Weight"
			,"Edn_Battery_Code"
			,"Edn_Battery_Item"
			,"Edn_Battery_Quantity"
			,"Vendor_No"
			,"Alternative_Item_No"
			,"Firma"
			,"load_ts"
		)
		SELECT
			i."No"
			,CONCAT(%L, '_', i."No")
			,i."baseUnitOfMeasure"
			,i."baseGroup"
			,i."blocked"
			,i."costingMethod"
			,REGEXP_REPLACE(i."description", '''', '', 'g')
			,REGEXP_REPLACE(i."description2", '''', '', 'g')
			,i."ednCoolingCapacityKW"
			,i."ednEfficiencyIndex"
			,i."ednFactorType"
			,i."ednHeatingCapacityKW"
			,i."ednNAVKey"
			,i."ednRefrigerant"
			,i."ednRefrigerantQuantityUoM"
			,i."ednTypeA"
			,i."ednTypeB"
			,i."genProdPostingGroup"
			,i."inventory"
			,REPLACE(i."inventoryPostingGroup", 'USLUGI', 'USŁUGI')
			,i."mkGLQuantity"
			,i."manufacturerCode"
			,i."maximumInventory"
			,i."maximumOrderQuantity"
			,i."minimumOrderQuantity"
			,i."negativeAdjmtLCY"
			,i."negativeAdjmtQty"
			,i."netChange"
			,i."netInvoicedQty"
			,i."no2"
			,i."qtyAssignedToShip"
			,i."qtyPicked"
			,i."qtyInTransit"
			,i."qtyOnAsmComponent"
			,i."qtyOnAssemblyOrder"
			,i."qtyOnComponentLines"
			,i."qtyOnJobOrder"
			,i."qtyOnProdOrder"
			,i."qtyOnPurchOrder"
			,i."qtyOnPurchReturn"
			,i."qtyOnSalesOrder"
			,i."qtyOnSalesReturn"
			,i."qtyOnServiceOrder"
			,i."relOrderReceiptQty"
			,i."reservedQtyOnInventory"
			,i."reservedQtyOnProdOrder"
			,i."reservedQtyOnPurchOrders"
			,i."reservedQtyOnSalesOrders"
			,i."systemId"
			,i."systemModifiedAt"
			,i."unitCost"
			,i."netWeight"
			,i."ednBatteryCode"
			,i."ednBatteryItem"
			,i."ednBatteryQuantity"
			,i."vendorNo"
			,i."alternativeItemNo"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I as i
		
    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_items()  -- ZMIENIĆ NAZWĘ FUNKCJI
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
	firma TEXT;
	litera_firmy CHAR(1);
--	tgt_schema TEXT := 'silver';
	target_table TEXT;
	v_unit_price_pln numeric(18, 2);

BEGIN
-- pobiera argumenty przekazane w CREATE TRIGGER 
	firma := TG_ARGV[0];
	litera_firmy := UPPER(SUBSTR(firma, 1, 1));
-- litera := TG_ARGV[1];
	target_table := format('bc_items_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --




EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"Key_Item_No"
		,"Base_Unit_Of_Measure"
		,"Base_Group"
		,"Blocked"
		,"Costing_Method"
		,"Description"
		,"Description_2"
		,"Edn_Cooling_Capacity_KW"
		,"Edn_Efficiency_Index"
		,"Edn_Factor_Type"
		,"Edn_Heating_Capacity_KW"
		,"Edn_NAV_Key"
		,"Edn_Refrigerant"
		,"Edn_Refrigerant_Quantity_UoM"
		,"Edn_Type_A"
		,"Edn_Type_B"
		,"Gen_Prod_Posting_Group"
		,"Inventory"
		,"Inventory_Posting_Group"
		,"Mk_GL_Quantity"
		,"Manufacturer_Code"
		,"Maximum_Inventory"
		,"Maximum_Order_Quantity"
		,"Minimum_Order_Quantity"
		,"Negative_Adjmt_LCY"
		,"Negative_Adjmt_Qty"
		,"Net_Change"
		,"Net_Invoiced_Qty"
		,"No_2"
		,"Qty_Assigned_To_Ship"
		,"Qty_Picked"
		,"Qty_In_Transit"
		,"Qty_On_Asm_Component"
		,"Qty_On_Assembly_Order"
		,"Qty_On_Component_Lines"
		,"Qty_On_Job_Order"
		,"Qty_On_Prod_Order"
		,"Qty_On_Purch_Order"
		,"Qty_On_Purch_Return"
		,"Qty_On_Sales_Order"
		,"Qty_On_Sales_Return"
		,"Qty_On_Service_Order"
		,"Rel_Order_Receipt_Qty"
		,"Reserved_Qty_On_Inventory"
		,"Reserved_Qty_On_Prod_Order"
		,"Reserved_Qty_On_Purch_Orders"
		,"Reserved_Qty_On_Sales_Orders"
		,"System_Id"
		,"System_Modified_At"
		,"Unit_Cost"
		,"Net_Weight"
		,"Edn_Battery_Code"
		,"Edn_Battery_Item"
		,"Edn_Battery_Quantity"
		,"Vendor_No"
		,"Alternative_Item_No"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30
		,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"No" = EXCLUDED."No"
		,"Key_Item_No" = EXCLUDED."Key_Item_No"
		,"baseUnitOfMeasure" = EXCLUDED."baseUnitOfMeasure"
		,"baseGroup" = EXCLUDED."baseGroup"
		,"blocked" = EXCLUDED."blocked"
		,"costingMethod" = EXCLUDED."costingMethod"
		,"description" = EXCLUDED."description"
		,"description2" = EXCLUDED."description2"
		,"ednCoolingCapacityKW" = EXCLUDED."ednCoolingCapacityKW"
		,"ednEfficiencyIndex" = EXCLUDED."ednEfficiencyIndex"
		,"ednFactorType" = EXCLUDED."ednFactorType"
		,"ednHeatingCapacityKW" = EXCLUDED."ednHeatingCapacityKW"
		,"ednNAVKey" = EXCLUDED."ednNAVKey"
		,"ednRefrigerant" = EXCLUDED."ednRefrigerant"
		,"ednRefrigerantQuantityUoM" = EXCLUDED."ednRefrigerantQuantityUoM"
		,"ednTypeA" = EXCLUDED."ednTypeA"
		,"ednTypeB" = EXCLUDED."ednTypeB"
		,"genProdPostingGroup" = EXCLUDED."genProdPostingGroup"
		,"inventory" = EXCLUDED."inventory"
		,"inventoryPostingGroup" = EXCLUDED."inventoryPostingGroup"
		,"mkGLQuantity" = EXCLUDED."mkGLQuantity"
		,"manufacturerCode" = EXCLUDED."manufacturerCode"
		,"maximumInventory" = EXCLUDED."maximumInventory"
		,"maximumOrderQuantity" = EXCLUDED."maximumOrderQuantity"
		,"minimumOrderQuantity" = EXCLUDED."minimumOrderQuantity"
		,"negativeAdjmtLCY" = EXCLUDED."negativeAdjmtLCY"
		,"negativeAdjmtQty" = EXCLUDED."negativeAdjmtQty"
		,"netChange" = EXCLUDED."netChange"
		,"netInvoicedQty" = EXCLUDED."netInvoicedQty"
		,"no2" = EXCLUDED."no2"
		,"qtyAssignedToShip" = EXCLUDED."qtyAssignedToShip"
		,"qtyPicked" = EXCLUDED."qtyPicked"
		,"qtyInTransit" = EXCLUDED."qtyInTransit"
		,"qtyOnAsmComponent" = EXCLUDED."qtyOnAsmComponent"
		,"qtyOnAssemblyOrder" = EXCLUDED."qtyOnAssemblyOrder"
		,"qtyOnComponentLines" = EXCLUDED."qtyOnComponentLines"
		,"qtyOnJobOrder" = EXCLUDED."qtyOnJobOrder"
		,"qtyOnProdOrder" = EXCLUDED."qtyOnProdOrder"
		,"qtyOnPurchOrder" = EXCLUDED."qtyOnPurchOrder"
		,"qtyOnPurchReturn" = EXCLUDED."qtyOnPurchReturn"
		,"qtyOnSalesOrder" = EXCLUDED."qtyOnSalesOrder"
		,"qtyOnSalesReturn" = EXCLUDED."qtyOnSalesReturn"
		,"qtyOnServiceOrder" = EXCLUDED."qtyOnServiceOrder"
		,"relOrderReceiptQty" = EXCLUDED."relOrderReceiptQty"
		,"reservedQtyOnInventory" = EXCLUDED."reservedQtyOnInventory"
		,"reservedQtyOnProdOrder" = EXCLUDED."reservedQtyOnProdOrder"
		,"reservedQtyOnPurchOrders" = EXCLUDED."reservedQtyOnPurchOrders"
		,"reservedQtyOnSalesOrders" = EXCLUDED."reservedQtyOnSalesOrders"
		,"systemId" = EXCLUDED."systemId"
		,"systemModifiedAt" = EXCLUDED."systemModifiedAt"
		,"unitCost" = EXCLUDED."unitCost"
		,"netWeight" = EXCLUDED."netWeight"
		,"ednBatteryCode" = EXCLUDED."ednBatteryCode"
		,"ednBatteryItem" = EXCLUDED."ednBatteryItem"
		,"ednBatteryQuantity" = EXCLUDED."ednBatteryQuantity"
		,"vendorNo" = EXCLUDED."vendorNo"
		,"alternativeItemNo" = EXCLUDED."alternativeItemNo"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,CONCAT(litera_firmy, '_', NEW."No")
		,NEW."baseUnitOfMeasure"
		,NEW."baseGroup"
		,NEW."blocked"
		,NEW."costingMethod"
		,REGEXP_REPLACE(NEW."description", '''', '', 'g')
		,REGEXP_REPLACE(NEW."description2", '''', '', 'g')
		,NEW."ednCoolingCapacityKW"
		,NEW."ednEfficiencyIndex"
		,NEW."ednFactorType"
		,NEW."ednHeatingCapacityKW"
		,NEW."ednNAVKey"
		,NEW."ednRefrigerant"
		,NEW."ednRefrigerantQuantityUoM"
		,NEW."ednTypeA"
		,NEW."ednTypeB"
		,NEW."genProdPostingGroup"
		,NEW."inventory"
		,REPLACE(NEW."inventoryPostingGroup", 'USLUGI', 'USŁUGI')
		,NEW."mkGLQuantity"
		,NEW."manufacturerCode"
		,NEW."maximumInventory"
		,NEW."maximumOrderQuantity"
		,NEW."minimumOrderQuantity"
		,NEW."negativeAdjmtLCY"
		,NEW."negativeAdjmtQty"
		,NEW."netChange"
		,NEW."netInvoicedQty"
		,NEW."no2"
		,NEW."qtyAssignedToShip"
		,NEW."qtyPicked"
		,NEW."qtyInTransit"
		,NEW."qtyOnAsmComponent"
		,NEW."qtyOnAssemblyOrder"
		,NEW."qtyOnComponentLines"
		,NEW."qtyOnJobOrder"
		,NEW."qtyOnProdOrder"
		,NEW."qtyOnPurchOrder"
		,NEW."qtyOnPurchReturn"
		,NEW."qtyOnSalesOrder"
		,NEW."qtyOnSalesReturn"
		,NEW."qtyOnServiceOrder"
		,NEW."relOrderReceiptQty"
		,NEW."reservedQtyOnInventory"
		,NEW."reservedQtyOnProdOrder"
		,NEW."reservedQtyOnPurchOrders"
		,NEW."reservedQtyOnSalesOrders"
		,NEW."systemId"
		,NEW."systemModifiedAt"
		,NEW."unitCost"
		,NEW."netWeight"
		,NEW."ednBatteryCode"
		,NEW."ednBatteryItem"
		,NEW."ednBatteryQuantity"
		,NEW."vendorNo"
		,NEW."alternativeItemNo"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON CUSTOMERS TABLE
------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'items'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon'];
	firma text;
BEGIN
	FOREACH firma IN ARRAY firmy LOOP
		EXECUTE format($sql$
			DROP TRIGGER IF EXISTS trg_upsert_bc_%I_%I ON bronze.bc_%I_%I;
			CREATE TRIGGER trg_upsert_bc_%I_%I
			AFTER INSERT OR UPDATE ON bronze.bc_%I_%I
			FOR EACH ROW
			EXECUTE FUNCTION bronze.fn_upsert_bc_%I(%L)
		$sql$, 
		grupa_tabel, firma,   -- dla DROP
		grupa_tabel, firma,   -- dla DROP
		grupa_tabel, firma,   -- dla CREATE
		grupa_tabel, firma,   -- dla CREATE
		grupa_tabel,          -- dla funkcji fn_upsert_bc_<grupa_tabel>
		firma                 -- parametr do funkcji jako tekst
		);
	END LOOP;
END;
$$ LANGUAGE plpgsql;