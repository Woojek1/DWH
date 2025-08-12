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
			,"baseUnitOfMeasure" text NULL
			,"baseGroup" text NULL
			,"blocked" bool NULL
			,"costingMethod" text NULL
			,"description" text NULL
			,"description2" text NULL
			,"ednCoolingCapacityKW" numeric(14,2) NULL
			,"ednEfficiencyIndex" numeric(14,2) NULL
			,"ednFactorType" text NULL
			,"ednHeatingCapacityKW" numeric(14,2) NULL
			,"ednNAVKey" text NULL
			,"ednRefrigerant" text NULL
			,"ednRefrigerantQuantityUoM" numeric(14,2) NULL
			,"ednTypeA" text NULL
			,"ednTypeB" text NULL
			,"genProdPostingGroup" text NULL
			,"inventory" numeric(14,2) NULL
			,"inventoryPostingGroup" text NULL
			,"mkGLQuantity" numeric(14, 2) NULL
			,"manufacturerCode" text NULL
			,"maximumInventory" numeric(14,2) NULL
			,"maximumOrderQuantity" numeric(14,2) NULL
			,"minimumOrderQuantity" numeric(14,2) NULL
			,"negativeAdjmtLCY" numeric(14,2) NULL
			,"negativeAdjmtQty" numeric(14,2) NULL
			,"netChange" numeric(14,2) NULL
			,"netInvoicedQty" numeric(14,2) NULL
			,"no2" text NULL
			,"qtyAssignedToShip" numeric(14,2) NULL
			,"qtyPicked" numeric(14,2) NULL
			,"qtyInTransit" numeric(14,2) NULL
			,"qtyOnAsmComponent" numeric(14,2) NULL
			,"qtyOnAssemblyOrder" numeric(14,2) NULL
			,"qtyOnComponentLines" numeric(14,2) NULL
			,"qtyOnJobOrder" numeric(14,2) NULL
			,"qtyOnProdOrder" numeric(14,2) NULL
			,"qtyOnPurchOrder" numeric(14,2) NULL
			,"qtyOnPurchReturn" numeric(14,2) NULL
			,"qtyOnSalesOrder" numeric(14,2) NULL
			,"qtyOnSalesReturn" numeric(14,2) NULL
			,"qtyOnServiceOrder" numeric(14,2) NULL
			,"relOrderReceiptQty" numeric(14,2) NULL
			,"reservedQtyOnInventory" numeric(14,2) NULL
			,"reservedQtyOnProdOrder" numeric(14,2) NULL
			,"reservedQtyOnPurchOrders" numeric(14,2) NULL
			,"reservedQtyOnSalesOrders" numeric(14,2) NULL
			,"systemId" text NULL
			,"systemModifiedAt" timestamptz NULL
			,"unitCost" numeric(14,2) NULL
			,"netWeight" numeric (14,2) NULL
			,"ednBatteryCode" text NULL
			,"ednBatteryItem" bool NULL
			,"ednBatteryQuantity" numeric(14, 2) NULL
			,"vendorNo" text NULL
			,"alternativeItemNo" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
 			,PRIMARY KEY ("No")
		);
	$ddl$, _tabela, _litera_firmy);


 
-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"baseUnitOfMeasure"
			,"baseGroup"
			,"blocked"
			,"costingMethod"
			,"description"
			,"description2"
			,"ednCoolingCapacityKW"
			,"ednEfficiencyIndex"
			,"ednFactorType"
			,"ednHeatingCapacityKW"
			,"ednNAVKey"
			,"ednRefrigerant"
			,"ednRefrigerantQuantityUoM"
			,"ednTypeA"
			,"ednTypeB"
			,"genProdPostingGroup"
			,"inventory"
			,"inventoryPostingGroup"
			,"mkGLQuantity"
			,"manufacturerCode"
			,"maximumInventory"
			,"maximumOrderQuantity"
			,"minimumOrderQuantity"
			,"negativeAdjmtLCY"
			,"negativeAdjmtQty"
			,"netChange"
			,"netInvoicedQty"
			,"no2"
			,"qtyAssignedToShip"
			,"qtyPicked"
			,"qtyInTransit"
			,"qtyOnAsmComponent"
			,"qtyOnAssemblyOrder"
			,"qtyOnComponentLines"
			,"qtyOnJobOrder"
			,"qtyOnProdOrder"
			,"qtyOnPurchOrder"
			,"qtyOnPurchReturn"
			,"qtyOnSalesOrder"
			,"qtyOnSalesReturn"
			,"qtyOnServiceOrder"
			,"relOrderReceiptQty"
			,"reservedQtyOnInventory"
			,"reservedQtyOnProdOrder"
			,"reservedQtyOnPurchOrders"
			,"reservedQtyOnSalesOrders"
			,"systemId"
			,"systemModifiedAt"
			,"unitCost"
			,"netWeight"
			,"ednBatteryCode"
			,"ednBatteryItem"
			,"ednBatteryQuantity"
			,"vendorNo"
			,"alternativeItemNo"
			,"Firma"
			,"load_ts"
		)
		SELECT
			i."No"
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
		
    $insert$, _tabela, _litera_firmy, _tabela);

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
		,"baseUnitOfMeasure"
		,"baseGroup"
		,"blocked"
		,"costingMethod"
		,"description"
		,"description2"
		,"ednCoolingCapacityKW"
		,"ednEfficiencyIndex"
		,"ednFactorType"
		,"ednHeatingCapacityKW"
		,"ednNAVKey"
		,"ednRefrigerant"
		,"ednRefrigerantQuantityUoM"
		,"ednTypeA"
		,"ednTypeB"
		,"genProdPostingGroup"
		,"inventory"
		,"inventoryPostingGroup"
		,"mkGLQuantity"
		,"manufacturerCode"
		,"maximumInventory"
		,"maximumOrderQuantity"
		,"minimumOrderQuantity"
		,"negativeAdjmtLCY"
		,"negativeAdjmtQty"
		,"netChange"
		,"netInvoicedQty"
		,"no2"
		,"qtyAssignedToShip"
		,"qtyPicked"
		,"qtyInTransit"
		,"qtyOnAsmComponent"
		,"qtyOnAssemblyOrder"
		,"qtyOnComponentLines"
		,"qtyOnJobOrder"
		,"qtyOnProdOrder"
		,"qtyOnPurchOrder"
		,"qtyOnPurchReturn"
		,"qtyOnSalesOrder"
		,"qtyOnSalesReturn"
		,"qtyOnServiceOrder"
		,"relOrderReceiptQty"
		,"reservedQtyOnInventory"
		,"reservedQtyOnProdOrder"
		,"reservedQtyOnPurchOrders"
		,"reservedQtyOnSalesOrders"
		,"systemId"
		,"systemModifiedAt"
		,"unitCost"
		,"netWeight"
		,"ednBatteryCode"
		,"ednBatteryItem"
		,"ednBatteryQuantity"
		,"vendorNo"
		,"alternativeItemNo"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30
		,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"No" = EXCLUDED."No"
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