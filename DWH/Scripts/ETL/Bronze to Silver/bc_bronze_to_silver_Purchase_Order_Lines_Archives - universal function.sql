-----------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES LINES TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------

 
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
	_tabela := format('bc_purchase_order_lines_archives_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NULL
			,"Document_No" text NOT NULL
			,"Key_Document_No" text NOT NULL
			,"Doc_No_Occurrence" int4 NOT NULL
			,"Version_No" int4 NOT NULL
			,"Line_No" int4 NOT NULL
			,"Type" text NULL
			,"No" text NULL
			,"Key_No_Item" text NULL
			,"Item_Reference_No" text NULL
			,"Variant_Code" text NULL
			,"Nonstock" bool NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"VAT_Bus_Posting_Group" text NULL
			,"VAT_Prod_Posting_Group" text NULL
			,"Description" text NULL
			,"Description_2" text NULL
			,"Drop_Shipment" bool NULL
			,"Return_Reason_Code" text NULL
			,"Location_Code" text NULL
			,"Quantity" numeric(18, 4) NULL
			,"Unit_of_Measure_Code" text NULL
			,"Unit_of_Measure" text NULL
			,"Direct_Unit_Cost" numeric(18, 4) NULL
			,"Indirect_Cost_Percent" numeric(5, 2) NULL
			,"Unit_Cost_LCY" numeric(18, 4) NULL
			,"Unit_Price_LCY" numeric(18, 4) NULL
			,"Tax_Liable" bool NULL
			,"Tax_Area_Code" text NULL
			,"Tax_Group_Code" text NULL
			,"Use_Tax" bool NULL
			,"Line_Amount" numeric(18, 4) NULL
			,"Line_Discount_Percent" numeric(5, 2) NULL
			,"Line_Discount_Amount" numeric(18, 4) NULL
			,"Allow_Invoice_Disc" bool NULL
			,"Inv_Discount_Amount" numeric(18, 4) NULL
			,"Qty_to_Receive" numeric(18, 4) NULL
			,"Quantity_Received" numeric(18, 4) NULL
			,"Qty_to_Invoice" numeric(18, 4) NULL
			,"Quantity_Invoiced" numeric(18, 4) NULL
			,"Allow_Item_Charge_Assignment" bool NULL
			,"Requested_Receipt_Date" date NULL
			,"Promised_Receipt_Date" date NULL
			,"Planned_Receipt_Date" date NULL
			,"Expected_Receipt_Date" date NULL
			,"Order_Date" date NULL
			,"Lead_Time_Calculation" text NULL
			,"Job_No" text NULL
			,"Job_Task_No" text NULL
			,"Job_Planning_Line_No" int4 NULL
			,"Job_Line_Type" text NULL
			,"Job_Unit_Price" numeric(18, 4) NULL
			,"Job_Line_Amount" numeric(18, 4) NULL
			,"Job_Line_Discount_Amount" numeric(18, 4) NULL
			,"Job_Line_Discount_Percent" numeric(5, 2) NULL
			,"Job_Total_Price" numeric(18, 4) NULL
			,"Job_Unit_Price_LCY" numeric(18, 4) NULL
			,"Job_Total_Price_LCY" numeric(18, 4) NULL
			,"Job_Line_Amount_LCY" numeric(18, 4) NULL
			,"Job_Line_Disc_Amount_LCY" numeric(18, 4) NULL
			,"Planning_Flexibility" text NULL
			,"Prod_Order_Line_No" int4 NULL
			,"Prod_Order_No" text NULL
			,"Operation_No" text NULL
			,"Work_Center_No" text NULL
			,"Finished" bool NULL
			,"Inbound_Whse_Handling_Time" text NULL
			,"Blanket_Order_No" text NULL
			,"Blanket_Order_Line_No" int4 NULL
			,"Appl_to_Item_Entry" int4 NULL
			,"Deferral_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"ITI_Service_Statistical_Code" text NULL
			,"ShortcutDimCode3" text NULL
			,"ShortcutDimCode4" text NULL
			,"ShortcutDimCode5" text NULL
			,"ShortcutDimCode6" text NULL
			,"ShortcutDimCode7" text NULL
			,"ShortcutDimCode8" text NULL
			,"Gross_Weight" numeric(18, 4) NULL
			,"Net_Weight" numeric(18, 4) NULL
			,"Unit_Volume" numeric(18, 4) NULL
			,"Units_per_Parcel" numeric(18, 4) NULL
			,"SystemCreatedAt" timetz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("Document_No", "Doc_No_Occurrence", "Version_No", "Line_No")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Doc_No_Occurrence"
			,"Version_No"
			,"Line_No"
			,"Type"
			,"No"
			,"Key_No_Item"
			,"Item_Reference_No"
			,"Variant_Code"
			,"Nonstock"
			,"Gen_Bus_Posting_Group"
			,"Gen_Prod_Posting_Group"
			,"VAT_Bus_Posting_Group"
			,"VAT_Prod_Posting_Group"
			,"Description"
			,"Description_2"
			,"Drop_Shipment"
			,"Return_Reason_Code"
			,"Location_Code"
			,"Quantity"
			,"Unit_of_Measure_Code"
			,"Unit_of_Measure"
			,"Direct_Unit_Cost"
			,"Indirect_Cost_Percent"
			,"Unit_Cost_LCY"
			,"Unit_Price_LCY"
			,"Tax_Liable"
			,"Tax_Area_Code"
			,"Tax_Group_Code"
			,"Use_Tax"
			,"Line_Amount"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Allow_Invoice_Disc"
			,"Inv_Discount_Amount"
			,"Qty_to_Receive"
			,"Quantity_Received"
			,"Qty_to_Invoice"
			,"Quantity_Invoiced"
			,"Allow_Item_Charge_Assignment"
			,"Requested_Receipt_Date"
			,"Promised_Receipt_Date"
			,"Planned_Receipt_Date"
			,"Expected_Receipt_Date"
			,"Order_Date"
			,"Lead_Time_Calculation"
			,"Job_No"
			,"Job_Task_No"
			,"Job_Planning_Line_No"
			,"Job_Line_Type"
			,"Job_Unit_Price"
			,"Job_Line_Amount"
			,"Job_Line_Discount_Amount"
			,"Job_Line_Discount_Percent"
			,"Job_Total_Price"
			,"Job_Unit_Price_LCY"
			,"Job_Total_Price_LCY"
			,"Job_Line_Amount_LCY"
			,"Job_Line_Disc_Amount_LCY"
			,"Planning_Flexibility"
			,"Prod_Order_Line_No"
			,"Prod_Order_No"
			,"Operation_No"
			,"Work_Center_No"
			,"Finished"
			,"Inbound_Whse_Handling_Time"
			,"Blanket_Order_No"
			,"Blanket_Order_Line_No"
			,"Appl_to_Item_Entry"
			,"Deferral_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"ITI_Service_Statistical_Code"
			,"ShortcutDimCode3"
			,"ShortcutDimCode4"
			,"ShortcutDimCode5"
			,"ShortcutDimCode6"
			,"ShortcutDimCode7"
			,"ShortcutDimCode8"
			,"Gross_Weight"
			,"Net_Weight"
			,"Unit_Volume"
			,"Units_per_Parcel"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pola."Document_Type"
			,pola."Document_No"
			,CONCAT(%L, '_', pola."Document_No")
			,pola."Doc_No_Occurrence"
			,pola."Version_No"
			,pola."Line_No"
			,pola."Type"
			,pola."No"
			,CONCAT(%L, '_', pola."No")
			,pola."Item_Reference_No"
			,pola."Variant_Code"
			,pola."Nonstock"
			,pola."Gen_Bus_Posting_Group"
			,pola."Gen_Prod_Posting_Group"
			,pola."VAT_Bus_Posting_Group"
			,pola."VAT_Prod_Posting_Group"
			,pola."Description"
			,pola."Description_2"
			,pola."Drop_Shipment"
			,pola."Return_Reason_Code"
			,pola."Location_Code"
			,pola."Quantity"
			,pola."Unit_of_Measure_Code"
			,pola."Unit_of_Measure"
			,pola."Direct_Unit_Cost"
			,pola."Indirect_Cost_Percent"
			,pola."Unit_Cost_LCY"
			,pola."Unit_Price_LCY"
			,pola."Tax_Liable"
			,pola."Tax_Area_Code"
			,pola."Tax_Group_Code"
			,pola."Use_Tax"
			,pola."Line_Amount"
			,pola."Line_Discount_Percent"
			,pola."Line_Discount_Amount"
			,pola."Allow_Invoice_Disc"
			,pola."Inv_Discount_Amount"
			,pola."Qty_to_Receive"
			,pola."Quantity_Received"
			,pola."Qty_to_Invoice"
			,pola."Quantity_Invoiced"
			,pola."Allow_Item_Charge_Assignment"
			,pola."Requested_Receipt_Date"
			,pola."Promised_Receipt_Date"
			,pola."Planned_Receipt_Date"
			,pola."Expected_Receipt_Date"
			,pola."Order_Date"
			,pola."Lead_Time_Calculation"
			,pola."Job_No"
			,pola."Job_Task_No"
			,pola."Job_Planning_Line_No"
			,pola."Job_Line_Type"
			,pola."Job_Unit_Price"
			,pola."Job_Line_Amount"
			,pola."Job_Line_Discount_Amount"
			,pola."Job_Line_Discount_Percent"
			,pola."Job_Total_Price"
			,pola."Job_Unit_Price_LCY"
			,pola."Job_Total_Price_LCY"
			,pola."Job_Line_Amount_LCY"
			,pola."Job_Line_Disc_Amount_LCY"
			,pola."Planning_Flexibility"
			,pola."Prod_Order_Line_No"
			,pola."Prod_Order_No"
			,pola."Operation_No"
			,pola."Work_Center_No"
			,pola."Finished"
			,pola."Inbound_Whse_Handling_Time"
			,pola."Blanket_Order_No"
			,pola."Blanket_Order_Line_No"
			,pola."Appl_to_Item_Entry"
			,pola."Deferral_Code"
			,pola."Shortcut_Dimension_1_Code"
			,pola."Shortcut_Dimension_2_Code"
			,pola."ITI_Service_Statistical_Code"
			,pola."ShortcutDimCode3"
			,pola."ShortcutDimCode4"
			,pola."ShortcutDimCode5"
			,pola."ShortcutDimCode6"
			,pola."ShortcutDimCode7"
			,pola."ShortcutDimCode8"
			,pola."Gross_Weight"
			,pola."Net_Weight"
			,pola."Unit_Volume"
			,pola."Units_per_Parcel"
			,pola."SystemCreatedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pola


    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_purchase_order_lines_archives()  -- ZMIENIĆ NAZWĘ FUNKCJI
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
	firma TEXT;
	litera_firmy CHAR(1);
--	tgt_schema TEXT := 'silver';
	target_table TEXT;

BEGIN
-- pobiera argumenty przekazane w CREATE TRIGGER 
	firma := TG_ARGV[0];
	litera_firmy := UPPER(SUBSTR(firma, 1, 1));
-- litera := TG_ARGV[1];
	target_table := format('bc_purchase_order_lines_archives_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Doc_No_Occurrence"
			,"Version_No"
			,"Line_No"
			,"Type"
			,"No"
			,"Key_No_Item"
			,"Item_Reference_No"
			,"Variant_Code"
			,"Nonstock"
			,"Gen_Bus_Posting_Group"
			,"Gen_Prod_Posting_Group"
			,"VAT_Bus_Posting_Group"
			,"VAT_Prod_Posting_Group"
			,"Description"
			,"Description_2"
			,"Drop_Shipment"
			,"Return_Reason_Code"
			,"Location_Code"
			,"Quantity"
			,"Unit_of_Measure_Code"
			,"Unit_of_Measure"
			,"Direct_Unit_Cost"
			,"Indirect_Cost_Percent"
			,"Unit_Cost_LCY"
			,"Unit_Price_LCY"
			,"Tax_Liable"
			,"Tax_Area_Code"
			,"Tax_Group_Code"
			,"Use_Tax"
			,"Line_Amount"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Allow_Invoice_Disc"
			,"Inv_Discount_Amount"
			,"Qty_to_Receive"
			,"Quantity_Received"
			,"Qty_to_Invoice"
			,"Quantity_Invoiced"
			,"Allow_Item_Charge_Assignment"
			,"Requested_Receipt_Date"
			,"Promised_Receipt_Date"
			,"Planned_Receipt_Date"
			,"Expected_Receipt_Date"
			,"Order_Date"
			,"Lead_Time_Calculation"
			,"Job_No"
			,"Job_Task_No"
			,"Job_Planning_Line_No"
			,"Job_Line_Type"
			,"Job_Unit_Price"
			,"Job_Line_Amount"
			,"Job_Line_Discount_Amount"
			,"Job_Line_Discount_Percent"
			,"Job_Total_Price"
			,"Job_Unit_Price_LCY"
			,"Job_Total_Price_LCY"
			,"Job_Line_Amount_LCY"
			,"Job_Line_Disc_Amount_LCY"
			,"Planning_Flexibility"
			,"Prod_Order_Line_No"
			,"Prod_Order_No"
			,"Operation_No"
			,"Work_Center_No"
			,"Finished"
			,"Inbound_Whse_Handling_Time"
			,"Blanket_Order_No"
			,"Blanket_Order_Line_No"
			,"Appl_to_Item_Entry"
			,"Deferral_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"ITI_Service_Statistical_Code"
			,"ShortcutDimCode3"
			,"ShortcutDimCode4"
			,"ShortcutDimCode5"
			,"ShortcutDimCode6"
			,"ShortcutDimCode7"
			,"ShortcutDimCode8"
			,"Gross_Weight"
			,"Net_Weight"
			,"Unit_Volume"
			,"Units_per_Parcel"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
	)
	SELECT 
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
		$41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
		$61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
		$81, $82, $83, $84, $85, $86, $87, $88   -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Document_No", "Doc_No_Occurrence", "Version_No", "Line_No") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"Document_No" = EXCLUDED."Document_No"
		,"Key_Document_No" = EXCLUDED."Key_Document_No"
		,"Doc_No_Occurrence" = EXCLUDED."Doc_No_Occurrence"
		,"Version_No" = EXCLUDED."Version_No"
		,"Line_No" = EXCLUDED."Line_No"
		,"Type" = EXCLUDED."Type"
		,"No" = EXCLUDED."No"
		,"Key_No_Item" = EXCLUDED."Key_No_Item"
		,"Item_Reference_No" = EXCLUDED."Item_Reference_No"
		,"Variant_Code" = EXCLUDED."Variant_Code"
		,"Nonstock" = EXCLUDED."Nonstock"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Gen_Prod_Posting_Group" = EXCLUDED."Gen_Prod_Posting_Group"
		,"VAT_Bus_Posting_Group" = EXCLUDED."VAT_Bus_Posting_Group"
		,"VAT_Prod_Posting_Group" = EXCLUDED."VAT_Prod_Posting_Group"
		,"Description" = EXCLUDED."Description"
		,"Description_2" = EXCLUDED."Description_2"
		,"Drop_Shipment" = EXCLUDED."Drop_Shipment"
		,"Return_Reason_Code" = EXCLUDED."Return_Reason_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Quantity" = EXCLUDED."Quantity"
		,"Unit_of_Measure_Code" = EXCLUDED."Unit_of_Measure_Code"
		,"Unit_of_Measure" = EXCLUDED."Unit_of_Measure"
		,"Direct_Unit_Cost" = EXCLUDED."Direct_Unit_Cost"
		,"Indirect_Cost_Percent" = EXCLUDED."Indirect_Cost_Percent"
		,"Unit_Cost_LCY" = EXCLUDED."Unit_Cost_LCY"
		,"Unit_Price_LCY" = EXCLUDED."Unit_Price_LCY"
		,"Tax_Liable" = EXCLUDED."Tax_Liable"
		,"Tax_Area_Code" = EXCLUDED."Tax_Area_Code"
		,"Tax_Group_Code" = EXCLUDED."Tax_Group_Code"
		,"Use_Tax" = EXCLUDED."Use_Tax"
		,"Line_Amount" = EXCLUDED."Line_Amount"
		,"Line_Discount_Percent" = EXCLUDED."Line_Discount_Percent"
		,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
		,"Allow_Invoice_Disc" = EXCLUDED."Allow_Invoice_Disc"
		,"Inv_Discount_Amount" = EXCLUDED."Inv_Discount_Amount"
		,"Qty_to_Receive" = EXCLUDED."Qty_to_Receive"
		,"Quantity_Received" = EXCLUDED."Quantity_Received"
		,"Qty_to_Invoice" = EXCLUDED."Qty_to_Invoice"
		,"Quantity_Invoiced" = EXCLUDED."Quantity_Invoiced"
		,"Allow_Item_Charge_Assignment" = EXCLUDED."Allow_Item_Charge_Assignment"
		,"Requested_Receipt_Date" = EXCLUDED."Requested_Receipt_Date"
		,"Promised_Receipt_Date" = EXCLUDED."Promised_Receipt_Date"
		,"Planned_Receipt_Date" = EXCLUDED."Planned_Receipt_Date"
		,"Expected_Receipt_Date" = EXCLUDED."Expected_Receipt_Date"
		,"Order_Date" = EXCLUDED."Order_Date"
		,"Lead_Time_Calculation" = EXCLUDED."Lead_Time_Calculation"
		,"Job_No" = EXCLUDED."Job_No"
		,"Job_Task_No" = EXCLUDED."Job_Task_No"
		,"Job_Planning_Line_No" = EXCLUDED."Job_Planning_Line_No"
		,"Job_Line_Type" = EXCLUDED."Job_Line_Type"
		,"Job_Unit_Price" = EXCLUDED."Job_Unit_Price"
		,"Job_Line_Amount" = EXCLUDED."Job_Line_Amount"
		,"Job_Line_Discount_Amount" = EXCLUDED."Job_Line_Discount_Amount"
		,"Job_Line_Discount_Percent" = EXCLUDED."Job_Line_Discount_Percent"
		,"Job_Total_Price" = EXCLUDED."Job_Total_Price"
		,"Job_Unit_Price_LCY" = EXCLUDED."Job_Unit_Price_LCY"
		,"Job_Total_Price_LCY" = EXCLUDED."Job_Total_Price_LCY"
		,"Job_Line_Amount_LCY" = EXCLUDED."Job_Line_Amount_LCY"
		,"Job_Line_Disc_Amount_LCY" = EXCLUDED."Job_Line_Disc_Amount_LCY"
		,"Planning_Flexibility" = EXCLUDED."Planning_Flexibility"
		,"Prod_Order_Line_No" = EXCLUDED."Prod_Order_Line_No"
		,"Prod_Order_No" = EXCLUDED."Prod_Order_No"
		,"Operation_No" = EXCLUDED."Operation_No"
		,"Work_Center_No" = EXCLUDED."Work_Center_No"
		,"Finished" = EXCLUDED."Finished"
		,"Inbound_Whse_Handling_Time" = EXCLUDED."Inbound_Whse_Handling_Time"
		,"Blanket_Order_No" = EXCLUDED."Blanket_Order_No"
		,"Blanket_Order_Line_No" = EXCLUDED."Blanket_Order_Line_No"
		,"Appl_to_Item_Entry" = EXCLUDED."Appl_to_Item_Entry"
		,"Deferral_Code" = EXCLUDED."Deferral_Code"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"ITI_Service_Statistical_Code" = EXCLUDED."ITI_Service_Statistical_Code"
		,"ShortcutDimCode3" = EXCLUDED."ShortcutDimCode3"
		,"ShortcutDimCode4" = EXCLUDED."ShortcutDimCode4"
		,"ShortcutDimCode5" = EXCLUDED."ShortcutDimCode5"
		,"ShortcutDimCode6" = EXCLUDED."ShortcutDimCode6"
		,"ShortcutDimCode7" = EXCLUDED."ShortcutDimCode7"
		,"ShortcutDimCode8" = EXCLUDED."ShortcutDimCode8"
		,"Gross_Weight" = EXCLUDED."Gross_Weight"
		,"Net_Weight" = EXCLUDED."Net_Weight"
		,"Unit_Volume" = EXCLUDED."Unit_Volume"
		,"Units_per_Parcel" = EXCLUDED."Units_per_Parcel"
		,"SystemCreatedAt" = EXCLUDED."SystemCreatedAt"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Document_Type"
		,NEW."No"
		,concat(litera_firmy, '_', NEW."No")
		,NEW."Doc_No_Occurrence"
		,NEW."Version_No"
		,NEW."Date_Archived"
		,NEW."Time_Archived"
		,NEW."Archived_By"
		,NEW."Interaction_Exist"
		,NEW."Buy_from_Vendor_No"
		,concat(litera_firmy, '_', NEW."Buy_from_Vendor_No")
		,NEW."Order_Address_Code"
		,NEW."Buy_from_Vendor_Name"
		,NEW."Vendor_Authorization_No"
		,NEW."Buy_from_Post_Code"
		,NEW."Buy_from_Country_Region_Code"
		,NEW."Buy_from_Contact"
		,NEW."Pay_to_Vendor_No"
		,NEW."Pay_to_Name"
		,NEW."Pay_to_Post_Code"
		,NEW."Pay_to_Country_Region_Code"
		,NEW."Pay_to_Contact"
		,NEW."Ship_to_Code"
		,NEW."Ship_to_Name"
		,NEW."Ship_to_Post_Code"
		,NEW."Ship_to_Country_Region_Code"
		,NEW."Ship_to_Contact"
		,NEW."Posting_Date"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Location_Code"
		,NEW."Purchaser_Code"
		,NEW."Currency_Code"
		,NEW."Document_Date"
		,NEW."Payment_Terms_Code"
		,NEW."Due_Date"
		,NEW."Payment_Discount_Percent"
		,NEW."Payment_Method_Code"
		,NEW."Shipment_Method_Code"
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
	grupa_tabel text := 'purchase_order_lines_archives'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon', 'zymetric', 'technab'];
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