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
	_tabela := format('bc_customer_ledger_entries_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Entry_No" int4 NOT NULL
			,"Posting_Date" date NULL
			,"Document_Date" date NULL
			,"Document_Type" text NULL
			,"Document_No" text NULL
			,"Customer_No" text NULL
			,"Customer_Name" text NULL
			,"Description" text NULL
			,"Global_Dimension_1_Code" text NULL
			,"Global_Dimension_2_Code" text NULL
			,"Customer_Posting_Group" text NULL
			,"IC_Partner_Code" text NULL
			,"Salesperson_Code" text NULL
			,"Currency_Code" text NULL
			,"Original_Amount" numeric(14, 2) NULL
			,"Original_Amt_LCY" numeric(14, 2) NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_LCY" numeric(14, 2) NULL
			,"Debit_Amount" numeric(14, 2) NULL
			,"Debit_Amount_LCY" numeric(14, 2) NULL
			,"Credit_Amount" numeric(14, 2) NULL
			,"Credit_Amount_LCY" numeric(14, 2) NULL
			,"Remaining_Amount" numeric(14, 2) NULL
			,"Remaining_Amt_LCY" numeric(14, 2) NULL
			,"Sales_LCY" numeric(14, 2) NULL
			,"Bal_Account_Type" text NULL
			,"Bal_Account_No" text NULL
			,"Due_Date" date NULL
			,"EDN_Days_Until_Due" int4 NULL
			,"EDN_Days_Overdue" int4 NULL
			,"Payment_Prediction" text NULL
			,"Prediction_Confidence" text NULL
			,"Prediction_Confidence_Percent" numeric(6, 2) NULL
			,"Pmt_Discount_Date" date NULL
			,"Pmt_Disc_Tolerance_Date" date NULL
			,"Original_Pmt_Disc_Possible" numeric(14, 2) NULL
			,"Remaining_Pmt_Disc_Possible" numeric(14, 2) NULL
			,"Max_Payment_Tolerance" numeric(14, 2) NULL
			,"Payment_Method_Code" text NULL
			,"Open" bool NULL
			,"Closed_at_Date" date NULL
			,"On_Hold" text NULL
			,"User_ID" text NULL
			,"Source_Code" text NULL
			,"Reason_Code" text NULL
			,"Reversed" bool NULL
			,"Reversed_by_Entry_No" int4 NULL
			,"Reversed_Entry_No" int4 NULL
			,"Exported_to_Payment_File" bool NULL
			,"Message_to_Recipient" text NULL
			,"Direct_Debit_Mandate_ID" text NULL
			,"Dimension_Set_ID" int4 NULL
			,"ITI_Split_Payment" bool NULL
			,"ITI_VAT_Transaction_Type" text NULL
			,"ITI_Bad_Debt_Rel_Corr_Amt_LCY" numeric(14, 2) NULL
			,"ITI_VAT_Entry_Amount" numeric(14, 2) NULL
			,"ITI_VAT_Entry_Base" numeric(14, 2) NULL
			,"External_Document_No" text NULL
			,"Your_Reference" text NULL
			,"RecipientBankAccount" text NULL
			,"Shortcut_Dimension_3_Code" text NULL
			,"Shortcut_Dimension_4_Code" text NULL
			,"Shortcut_Dimension_5_Code" text NULL
			,"Shortcut_Dimension_6_Code" text NULL
			,"Shortcut_Dimension_7_Code" text NULL
			,"Shortcut_Dimension_8_Code" text NULL
			,"EDN_Factoring_Invoice" bool NULL
			,"EDN_Insurance_Invoice" bool NULL
			,"EDN_KUKE_Symbol" text NULL
			,"EDN_Policy" text NULL
			,"EDN_Insurance_Due_Date" date NULL
			,"SystemModifiedAt" timestamp NULL
			,"SystemCreatedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
 			,PRIMARY KEY ("Entry_No")
		);
	$ddl$, _tabela, _litera_firmy);

 
 
-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Entry_No"
			,"Posting_Date"
			,"Document_Date"
			,"Document_Type"
			,"Document_No"
			,"Customer_No"
			,"Customer_Name"
			,"Description"
			,"Global_Dimension_1_Code"
			,"Global_Dimension_2_Code"
			,"Customer_Posting_Group"
			,"IC_Partner_Code"
			,"Salesperson_Code"
			,"Currency_Code"
			,"Original_Amount"
			,"Original_Amt_LCY"
			,"Amount"
			,"Amount_LCY"
			,"Debit_Amount"
			,"Debit_Amount_LCY"
			,"Credit_Amount"
			,"Credit_Amount_LCY"
			,"Remaining_Amount"
			,"Remaining_Amt_LCY"
			,"Sales_LCY"
			,"Bal_Account_Type"
			,"Bal_Account_No"
			,"Due_Date"
			,"EDN_Days_Until_Due"
			,"EDN_Days_Overdue"
			,"Payment_Prediction"
			,"Prediction_Confidence"
			,"Prediction_Confidence_Percent"
			,"Pmt_Discount_Date"
			,"Pmt_Disc_Tolerance_Date"
			,"Original_Pmt_Disc_Possible"
			,"Remaining_Pmt_Disc_Possible"
			,"Max_Payment_Tolerance"
			,"Payment_Method_Code"
			,"Open"
			,"Closed_at_Date"
			,"On_Hold"
			,"User_ID"
			,"Source_Code"
			,"Reason_Code"
			,"Reversed"
			,"Reversed_by_Entry_No"
			,"Reversed_Entry_No"
			,"Exported_to_Payment_File"
			,"Message_to_Recipient"
			,"Direct_Debit_Mandate_ID"
			,"Dimension_Set_ID"
			,"ITI_Split_Payment"
			,"ITI_VAT_Transaction_Type"
			,"ITI_Bad_Debt_Rel_Corr_Amt_LCY"
			,"ITI_VAT_Entry_Amount"
			,"ITI_VAT_Entry_Base"
			,"External_Document_No"
			,"Your_Reference"
			,"RecipientBankAccount"
			,"Shortcut_Dimension_3_Code"
			,"Shortcut_Dimension_4_Code"
			,"Shortcut_Dimension_5_Code"
			,"Shortcut_Dimension_6_Code"
			,"Shortcut_Dimension_7_Code"
			,"Shortcut_Dimension_8_Code"
			,"EDN_Factoring_Invoice"
			,"EDN_Insurance_Invoice"
			,"EDN_KUKE_Symbol"
			,"EDN_Policy"
			,"EDN_Insurance_Due_Date"
			,"SystemModifiedAt"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			cle."Entry_No"
			,cle."Posting_Date"
			,cle."Document_Date"
			,cle."Document_Type"
			,cle."Document_No"
			,cle."Customer_No"
			,cle."Customer_Name"
			,cle."Description"
			,cle."Global_Dimension_1_Code"
			,cle."Global_Dimension_2_Code"
			,cle."Customer_Posting_Group"
			,cle."IC_Partner_Code"
			,cle."Salesperson_Code"
			,cle."Currency_Code"
			,cle."Original_Amount"
			,cle."Original_Amt_LCY"
			,cle."Amount"
			,cle."Amount_LCY"
			,cle."Debit_Amount"
			,cle."Debit_Amount_LCY"
			,cle."Credit_Amount"
			,cle."Credit_Amount_LCY"
			,cle."Remaining_Amount"
			,cle."Remaining_Amt_LCY"
			,cle."Sales_LCY"
			,cle."Bal_Account_Type"
			,cle."Bal_Account_No"
			,cle."Due_Date"
			,cle."EDN_Days_Until_Due"
			,cle."EDN_Days_Overdue"
			,cle."Payment_Prediction"
			,cle."Prediction_Confidence"
			,cle."Prediction_Confidence_Percent"
			,cle."Pmt_Discount_Date"
			,cle."Pmt_Disc_Tolerance_Date"
			,cle."Original_Pmt_Disc_Possible"
			,cle."Remaining_Pmt_Disc_Possible"
			,cle."Max_Payment_Tolerance"
			,cle."Payment_Method_Code"
			,cle."Open"
			,CASE 
				WHEN cle."Open" IS FALSE THEN cle."Closed_at_Date"
				ELSE NULL
			END AS "Closed_at_Date"
			,cle."On_Hold"
			,cle."User_ID"
			,cle."Source_Code"
			,cle."Reason_Code"
			,cle."Reversed"
			,cle."Reversed_by_Entry_No"
			,cle."Reversed_Entry_No"
			,cle."Exported_to_Payment_File"
			,cle."Message_to_Recipient"
			,cle."Direct_Debit_Mandate_ID"
			,cle."Dimension_Set_ID"
			,cle."ITI_Split_Payment"
			,cle."ITI_VAT_Transaction_Type"
			,cle."ITI_Bad_Debt_Rel_Corr_Amt_LCY"
			,cle."ITI_VAT_Entry_Amount"
			,cle."ITI_VAT_Entry_Base"
			,cle."External_Document_No"
			,cle."Your_Reference"
			,cle."RecipientBankAccount"
			,cle."Shortcut_Dimension_3_Code"
			,cle."Shortcut_Dimension_4_Code"
			,cle."Shortcut_Dimension_5_Code"
			,cle."Shortcut_Dimension_6_Code"
			,cle."Shortcut_Dimension_7_Code"
			,cle."Shortcut_Dimension_8_Code"
			,cle."EDN_Factoring_Invoice"
			,cle."EDN_Insurance_Invoice"
			,cle."EDN_KUKE_Symbol"
			,cle."EDN_Policy"
			,cle."EDN_Insurance_Due_Date"
			,cle."SystemModifiedAt"
			,cle."SystemCreatedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I cle
		
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_customer_ledger_entries()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_customer_ledger_entries_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --




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
	grupa_tabel text := 'bc_customer_ledger_entries'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon', 'technab', 'zymetric'];
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