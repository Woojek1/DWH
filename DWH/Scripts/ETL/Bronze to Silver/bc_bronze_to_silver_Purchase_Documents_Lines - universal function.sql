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
	_tabela := format('bc_purchase_document_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---

 
-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NOT NULL
			,"Document_No" text NOT NULL
			,"Key_Document_No" text NOT NULL
			,"Line_Number" int4 NOT NULL
			,"Buy_From_Vendor_No" text NULL
			,"Key_Buy_From_Vendor_No" text NULL
			,"Type" text NULL
			,"No" text NULL
			,"Key_Item_No" text NULL
			,"Location_Code" text NULL
			,"Posting_Group" text NULL
			,"Expected_Receipt_Date" date NULL
			,"Description" text NULL
			,"Description_2" text NULL
			,"Unit_Of_Measure" text NULL
			,"Quantity" numeric(14, 2) NULL
			,"Outstanding_Quantity" numeric(14, 2) NULL
			,"Qty_To_Invoice" numeric(14, 2) NULL
			,"Qty_To_Receive" numeric(14, 2) NULL
			,"Direct_Unit_Cost" numeric(14, 2) NULL
			,"Unit_Cost_Lcy" numeric(14, 2) NULL
			,"Vat_Percent" numeric(6, 2) NULL
			,"Line_Discount_Percent" numeric(6, 2) NULL
			,"Line_Discount_Amount" numeric(14, 2) NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_Including_Vat" numeric(14, 2) NULL
			,"Unit_Price_Lcy" numeric(14, 2) NULL
			,"Allow_Invoice_Disc" bool NULL
			,"Gross_Weight" numeric(14, 2) NULL
			,"Net_Weight" numeric(14, 2) NULL
			,"Units_Per_Parcel" int4 NULL
			,"Unit_Volume" numeric(14, 2) NULL
			,"Appl_To_Item_Entry" int4 NULL
			,"Shortcut_Dimension1_Code" text NULL
			,"Shortcut_Dimension2_Code" text NULL
			,"Job_Number" text NULL
			,"Indirect_Cost_Percent" numeric(6, 2) NULL
			,"Recalculate_Invoice_Disc" bool NULL
			,"Outstanding_Amount" numeric(14, 2) NULL
			,"Qty_Rcd_Not_Invoiced" numeric(14, 2) NULL
			,"Amt_Rcd_Not_Invoiced" numeric(14, 2) NULL
			,"Quantity_Received" numeric(14, 2) NULL
			,"Quantity_Invoiced" numeric(14, 2) NULL
			,"Receipt_Number" text NULL
			,"Receipt_Line_Number" int4 NULL
			,"Profit_Percent" numeric(6, 2) NULL
			,"Pay_To_Vendor_Number" text NULL
			,"Inv_Discount_Amount" numeric(14, 2) NULL
			,"Vendor_Item_Number" text NULL
			,"Sales_Order_Number" text NULL
			,"Sales_Order_Line_Number" int4 NULL
			,"Drop_Shipment" bool NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"Vat_Calculation_Type" text NULL
			,"Transaction_Type" text NULL
			,"Transport_Method" text NULL
			,"Attached_To_Line_Number" int4 NULL
			,"Entry_Point" text NULL
			,"Area" text NULL
			,"Transaction_Specification" text NULL
			,"Tax_Area_Code" text NULL
			,"Tax_Liable" bool NULL
			,"Tax_Group_Code" text NULL
			,"Use_Tax" bool NULL
			,"Vat_Bus_Posting_Group" text NULL
			,"Vat_Prod_Posting_Group" text NULL
			,"Currency_Code" text NULL
			,"Outstanding_Amount_Lcy" numeric(14, 2) NULL
			,"Amt_Rcd_Not_Invoiced_Lcy" numeric(14, 2) NULL
			,"Reserved_Quantity" numeric(14, 2) NULL
			,"Blanket_Order_Number" text NULL
			,"Blanket_Order_Line_Number" int4 NULL
			,"Vat_Base_Amount" numeric(14, 2) NULL
			,"Unit_Cost" numeric(14, 2) NULL
			,"System_Created_Entry" bool NULL
			,"Line_Amount" numeric(14, 2) NULL
			,"Vat_Difference" numeric(14, 2) NULL
			,"Inv_Disc_Amount_To_Invoice" numeric(14, 2) NULL
			,"Vat_Identifier" text NULL
			,"Ic_Partner_Ref_Type" text NULL
			,"Ic_Partner_Reference" text NULL
			,"Prepayment_Percent" numeric(6, 2) NULL
			,"Prepmt_Line_Amount" numeric(14, 2) NULL
			,"Prepmt_Amt_Inv" numeric(14, 2) NULL
			,"Prepmt_Amt_Incl_Vat" numeric(14, 2) NULL
			,"Prepayment_Amount" numeric(14, 2) NULL
			,"Prepmt_Vat_Base_Amt" numeric(14, 2) NULL
			,"Prepayment_Vat_Percent" numeric(6, 2) NULL
			,"Prepmt_Vat_Calc_Type" text NULL
			,"Prepayment_Vat_Identifier" text NULL
			,"Prepayment_Tax_Area_Code" text NULL
			,"Prepayment_Tax_Liable" bool NULL
			,"Prepayment_Tax_Group_Code" text NULL
			,"Prepmt_Amt_To_Deduct" numeric(14, 2) NULL
			,"Prepmt_Amt_Deducted" numeric(14, 2) NULL
			,"Prepayment_Line" bool NULL
			,"Prepmt_Amount_Inv_Incl_Vat" numeric(14, 2) NULL
			,"Prepmt_Amount_Inv_Lcy" numeric(14, 2) NULL
			,"Ic_Partner_Code" text NULL
			,"Prepmt_Vat_Amount_Inv_Lcy" numeric(14, 2) NULL
			,"Prepayment_Vat_Difference" numeric(14, 2) NULL
			,"Prepmt_Vat_Diff_To_Deduct" numeric(14, 2) NULL
			,"Prepmt_Vat_Diff_Deducted" numeric(14, 2) NULL
			,"Outstanding_Amt_Ex_Vat_Lcy" numeric(14, 2) NULL
			,"ARcd_Not_Inv_Ex_Vat_Lcy" numeric(14, 2) NULL
			,"Dimension_Set_Id" int4 NULL
			,"Job_Task_Number" text NULL
			,"Job_Line_Type" text NULL
			,"Job_Unit_Price" numeric(14, 2) NULL
			,"Job_Total_Price" numeric(14, 2) NULL
			,"Job_Line_Amount" numeric(14, 2) NULL
			,"Job_Line_Discount_Amount" numeric(14, 2) NULL
			,"Job_Line_Discount_Percent" numeric(6, 2) NULL
			,"Job_Unit_Price_Lcy" numeric(14, 2) NULL
			,"Job_Total_Price_Lcy" numeric(14, 2) NULL
			,"Job_Line_Amount_Lcy" numeric(14, 2) NULL
			,"Job_Line_Disc_Amount_Lcy" numeric(14, 2) NULL
			,"Job_Currency_Factor" numeric(14, 2) NULL
			,"Job_Currency_Code" text NULL
			,"Job_Planning_Line_Number" int4 NULL
			,"Job_Remaining_Qty" numeric(14, 2) NULL
			,"Job_Remaining_Qty_Base" numeric(14, 2) NULL
			,"Deferral_Code" text NULL
			,"Returns_Deferral_Start_Date" date NULL
			,"Prod_Order_Number" text NULL
			,"Variant_Code" text NULL
			,"Bin_Code" text NULL
			,"Qty_Per_Unit_Of_Measure" numeric(14, 2) NULL
			,"Unit_Of_Measure_Code" text NULL
			,"Quantity_Base" numeric(14, 2) NULL
			,"Outstanding_Qty_Base" numeric(14, 2) NULL
			,"Qty_To_Invoice_Base" numeric(14, 2) NULL
			,"Qty_To_Receive_Base" numeric(14, 2) NULL
			,"Qty_Rcd_Not_Invoiced_Base" numeric(14, 2) NULL
			,"Qty_Received_Base" numeric(14, 2) NULL
			,"Qty_Invoiced_Base" numeric(14, 2) NULL
			,"Reserved_Qty_Base" numeric(14, 2) NULL
			,"Fa_Posting_Date" date NULL
			,"Fa_Posting_Type" text NULL
			,"Depreciation_Book_Code" text NULL
			,"Salvage_Value" numeric(14, 2) NULL
			,"Depr_Until_Fa_Posting_Date" bool NULL
			,"Depr_Acquisition_Cost" bool NULL
			,"Maintenance_Code" text NULL
			,"Insurance_Number" text NULL
			,"Budgeted_Fa_Number" text NULL
			,"Duplicate_In_Depreciation_Book" text NULL
			,"Use_Duplication_List" bool NULL
			,"Responsibility_Center" text NULL
			,"Item_Reference_Number" text NULL
			,"Item_Ref_Unit_Of_Measure" text NULL
			,"Item_Reference_Type" text NULL
			,"Item_Reference_Type_Number" text NULL
			,"Item_Category_Code" text NULL
			,"Nonstock" bool NULL
			,"Purchasing_Code" text NULL
			,"Special_Order" bool NULL
			,"Special_Order_Sales_Number" text NULL
			,"Special_Order_Sales_Line_Number" int4 NULL
			,"Whse_Outstanding_Qty_Base" numeric(14, 2) NULL
			,"Completely_Received" bool NULL
			,"Requested_Receipt_Date" date NULL
			,"Promised_Receipt_Date" date NULL
			,"Lead_Time_Calculation" text NULL
			,"Inbound_Whse_Handling_Time" text NULL
			,"Planned_Receipt_Date" date NULL
			,"Order_Date" date NULL
			,"Allow_Item_Charge_Assignment" bool NULL
			,"Qty_To_Assign" numeric(14, 2) NULL
			,"Qty_Assigned" numeric(14, 2) NULL
			,"Return_Qty_To_Ship" numeric(14, 2) NULL
			,"Return_Qty_To_Ship_Base" numeric(14, 2) NULL
			,"Return_Qty_Shipped_Not_Invd" numeric(14, 2) NULL
			,"Ret_Qty_Shpd_Not_Invd_Base" numeric(14, 2) NULL
			,"Return_Shpd_Not_Invd" numeric(14, 2) NULL
			,"Return_Shpd_Not_Invd_Lcy" numeric(14, 2) NULL
			,"Return_Qty_Shipped" numeric(14, 2) NULL
			,"Return_Qty_Shipped_Base" numeric(14, 2) NULL
			,"Return_Shipment_Number" text NULL
			,"Return_Shipment_Line_Number" int4 NULL
			,"Return_Reason_Code" text NULL
			,"Subtype" text NULL
			,"Routing_Number" text NULL
			,"Operation_Number" text NULL
			,"Work_Center_Number" text NULL
			,"Finished" bool NULL
			,"Prod_Order_Line_Number" int4 NULL
			,"Overhead_Rate" numeric(6, 2) NULL
			,"Mps_Order" bool NULL
			,"Planning_Flexibility" text NULL
			,"Safety_Lead_Time" text NULL
			,"Routing_Reference_Number" int4 NULL
			,"System_Modified_At" timestamptz NULL
			,"Firma" char(1)
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("Line_Number", "Document_No", "Document_Type")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Line_Number"
			,"Buy_From_Vendor_No"
			,"Key_Buy_From_Vendor_No"
			,"Type"
			,"No"
			,"Key_Item_No"
			,"Location_Code"
			,"Posting_Group"
			,"Expected_Receipt_Date"
			,"Description"
			,"Description_2"
			,"Unit_Of_Measure"
			,"Quantity"
			,"Outstanding_Quantity"
			,"Qty_To_Invoice"
			,"Qty_To_Receive"
			,"Direct_Unit_Cost"
			,"Unit_Cost_Lcy"
			,"Vat_Percent"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Amount"
			,"Amount_Including_Vat"
			,"Unit_Price_Lcy"
			,"Allow_Invoice_Disc"
			,"Gross_Weight"
			,"Net_Weight"
			,"Units_Per_Parcel"
			,"Unit_Volume"
			,"Appl_To_Item_Entry"
			,"Shortcut_Dimension1_Code"
			,"Shortcut_Dimension2_Code"
			,"Job_Number"
			,"Indirect_Cost_Percent"
			,"Recalculate_Invoice_Disc"
			,"Outstanding_Amount"
			,"Qty_Rcd_Not_Invoiced"
			,"Amt_Rcd_Not_Invoiced"
			,"Quantity_Received"
			,"Quantity_Invoiced"
			,"Receipt_Number"
			,"Receipt_Line_Number"
			,"Profit_Percent"
			,"Pay_To_Vendor_Number"
			,"Inv_Discount_Amount"
			,"Vendor_Item_Number"
			,"Sales_Order_Number"
			,"Sales_Order_Line_Number"
			,"Drop_Shipment"
			,"Gen_Bus_Posting_Group"
			,"Gen_Prod_Posting_Group"
			,"Vat_Calculation_Type"
			,"Transaction_Type"
			,"Transport_Method"
			,"Attached_To_Line_Number"
			,"Entry_Point"
			,"Area"
			,"Transaction_Specification"
			,"Tax_Area_Code"
			,"Tax_Liable"
			,"Tax_Group_Code"
			,"Use_Tax"
			,"Vat_Bus_Posting_Group"
			,"Vat_Prod_Posting_Group"
			,"Currency_Code"
			,"Outstanding_Amount_Lcy"
			,"Amt_Rcd_Not_Invoiced_Lcy"
			,"Reserved_Quantity"
			,"Blanket_Order_Number"
			,"Blanket_Order_Line_Number"
			,"Vat_Base_Amount"
			,"Unit_Cost"
			,"System_Created_Entry"
			,"Line_Amount"
			,"Vat_Difference"
			,"Inv_Disc_Amount_To_Invoice"
			,"Vat_Identifier"
			,"Ic_Partner_Ref_Type"
			,"Ic_Partner_Reference"
			,"Prepayment_Percent"
			,"Prepmt_Line_Amount"
			,"Prepmt_Amt_Inv"
			,"Prepmt_Amt_Incl_Vat"
			,"Prepayment_Amount"
			,"Prepmt_Vat_Base_Amt"
			,"Prepayment_Vat_Percent"
			,"Prepmt_Vat_Calc_Type"
			,"Prepayment_Vat_Identifier"
			,"Prepayment_Tax_Area_Code"
			,"Prepayment_Tax_Liable"
			,"Prepayment_Tax_Group_Code"
			,"Prepmt_Amt_To_Deduct"
			,"Prepmt_Amt_Deducted"
			,"Prepayment_Line"
			,"Prepmt_Amount_Inv_Incl_Vat"
			,"Prepmt_Amount_Inv_Lcy"
			,"Ic_Partner_Code"
			,"Prepmt_Vat_Amount_Inv_Lcy"
			,"Prepayment_Vat_Difference"
			,"Prepmt_Vat_Diff_To_Deduct"
			,"Prepmt_Vat_Diff_Deducted"
			,"Outstanding_Amt_Ex_Vat_Lcy"
			,"ARcd_Not_Inv_Ex_Vat_Lcy"
			,"Dimension_Set_Id"
			,"Job_Task_Number"
			,"Job_Line_Type"
			,"Job_Unit_Price"
			,"Job_Total_Price"
			,"Job_Line_Amount"
			,"Job_Line_Discount_Amount"
			,"Job_Line_Discount_Percent"
			,"Job_Unit_Price_Lcy"
			,"Job_Total_Price_Lcy"
			,"Job_Line_Amount_Lcy"
			,"Job_Line_Disc_Amount_Lcy"
			,"Job_Currency_Factor"
			,"Job_Currency_Code"
			,"Job_Planning_Line_Number"
			,"Job_Remaining_Qty"
			,"Job_Remaining_Qty_Base"
			,"Deferral_Code"
			,"Returns_Deferral_Start_Date"
			,"Prod_Order_Number"
			,"Variant_Code"
			,"Bin_Code"
			,"Qty_Per_Unit_Of_Measure"
			,"Unit_Of_Measure_Code"
			,"Quantity_Base"
			,"Outstanding_Qty_Base"
			,"Qty_To_Invoice_Base"
			,"Qty_To_Receive_Base"
			,"Qty_Rcd_Not_Invoiced_Base"
			,"Qty_Received_Base"
			,"Qty_Invoiced_Base"
			,"Reserved_Qty_Base"
			,"Fa_Posting_Date"
			,"Fa_Posting_Type"
			,"Depreciation_Book_Code"
			,"Salvage_Value"
			,"Depr_Until_Fa_Posting_Date"
			,"Depr_Acquisition_Cost"
			,"Maintenance_Code"
			,"Insurance_Number"
			,"Budgeted_Fa_Number"
			,"Duplicate_In_Depreciation_Book"
			,"Use_Duplication_List"
			,"Responsibility_Center"
			,"Item_Reference_Number"
			,"Item_Ref_Unit_Of_Measure"
			,"Item_Reference_Type"
			,"Item_Reference_Type_Number"
			,"Item_Category_Code"
			,"Nonstock"
			,"Purchasing_Code"
			,"Special_Order"
			,"Special_Order_Sales_Number"
			,"Special_Order_Sales_Line_Number"
			,"Whse_Outstanding_Qty_Base"
			,"Completely_Received"
			,"Requested_Receipt_Date"
			,"Promised_Receipt_Date"
			,"Lead_Time_Calculation"
			,"Inbound_Whse_Handling_Time"
			,"Planned_Receipt_Date"
			,"Order_Date"
			,"Allow_Item_Charge_Assignment"
			,"Qty_To_Assign"
			,"Qty_Assigned"
			,"Return_Qty_To_Ship"
			,"Return_Qty_To_Ship_Base"
			,"Return_Qty_Shipped_Not_Invd"
			,"Ret_Qty_Shpd_Not_Invd_Base"
			,"Return_Shpd_Not_Invd"
			,"Return_Shpd_Not_Invd_Lcy"
			,"Return_Qty_Shipped"
			,"Return_Qty_Shipped_Base"
			,"Return_Shipment_Number"
			,"Return_Shipment_Line_Number"
			,"Return_Reason_Code"
			,"Subtype"
			,"Routing_Number"
			,"Operation_Number"
			,"Work_Center_Number"
			,"Finished"
			,"Prod_Order_Line_Number"
			,"Overhead_Rate"
			,"Mps_Order"
			,"Planning_Flexibility"
			,"Safety_Lead_Time"
			,"Routing_Reference_Number"
			,"System_Modified_At"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pdl."documentType",
			pdl."documentNumber",
			CONCAT(%L, '_', pdl."documentNumber"),
			pdl."lineNumber",
			pdl."buyFromVendorNumber",
			CONCAT(%L, '_', pdl."buyFromVendorNumber"),
			pdl."type",
			pdl."number",
			CONCAT(%L, '_', pdl."number"),
			pdl."locationCode",
			pdl."postingGroup",
			pdl."expectedReceiptDate",
			pdl."description",
			pdl."description2",
			pdl."unitOfMeasure",
			pdl."quantity",
			pdl."outstandingQuantity",
			pdl."qtyToInvoice",
			pdl."qtyToReceive",
			pdl."directUnitCost",
			pdl."unitCostLcy",
			pdl."vatPercent",
			pdl."lineDiscountPercent",
			pdl."lineDiscountAmount",
			pdl.amount,
			pdl."amountIncludingVat",
			pdl."unitPriceLcy",
			pdl."allowInvoiceDisc",
			pdl."grossWeight",
			pdl."netWeight",
			pdl."unitsPerParcel",
			pdl."unitVolume",
			pdl."applToItemEntry",
			pdl."shortcutDimension1Code",
			pdl."shortcutDimension2Code",
			pdl."jobNumber",
			pdl."indirectCostPercent",
			pdl."recalculateInvoiceDisc",
			pdl."outstandingAmount",
			pdl."qtyRcdNotInvoiced",
			pdl."amtRcdNotInvoiced",
			pdl."quantityReceived",
			pdl."quantityInvoiced",
			pdl."receiptNumber",
			pdl."receiptLineNumber",
			pdl."profitPercent",
			pdl."payToVendorNumber",
			pdl."invDiscountAmount",
			pdl."vendorItemNumber",
			pdl."salesOrderNumber",
			pdl."salesOrderLineNumber",
			pdl."dropShipment",
			pdl."genBusPostingGroup",
			pdl."genProdPostingGroup",
			pdl."vatCalculationType",
			pdl."transactionType",
			pdl."transportMethod",
			pdl."attachedToLineNumber",
			pdl."entryPoint",
			pdl.area,
			pdl."transactionSpecification",
			pdl."taxAreaCode",
			pdl."taxLiable",
			pdl."taxGroupCode",
			pdl."useTax",
			pdl."vatBusPostingGroup",
			pdl."vatProdPostingGroup",
			pdl."currencyCode",
			pdl."outstandingAmountLcy",
			pdl."amtRcdNotInvoicedLcy",
			pdl."reservedQuantity",
			pdl."blanketOrderNumber",
			pdl."blanketOrderLineNumber",
			pdl."vatBaseAmount",
			pdl."unitCost",
			pdl."systemCreatedEntry",
			pdl."lineAmount",
			pdl."vatDifference",
			pdl."invDiscAmountToInvoice",
			pdl."vatIdentifier",
			pdl."icPartnerRefType",
			pdl."icPartnerReference",
			pdl."prepaymentPercent",
			pdl."prepmtLineAmount",
			pdl."prepmtAmtInv",
			pdl."prepmtAmtInclVat",
			pdl."prepaymentAmount",
			pdl."prepmtVatBaseAmt",
			pdl."prepaymentVatPercent",
			pdl."prepmtVatCalcType",
			pdl."prepaymentVatIdentifier",
			pdl."prepaymentTaxAreaCode",
			pdl."prepaymentTaxLiable",
			pdl."prepaymentTaxGroupCode",
			pdl."prepmtAmtToDeduct",
			pdl."prepmtAmtDeducted",
			pdl."prepaymentLine",
			pdl."prepmtAmountInvInclVat",
			pdl."prepmtAmountInvLcy",
			pdl."icPartnerCode",
			pdl."prepmtVatAmountInvLcy",
			pdl."prepaymentVatDifference",
			pdl."prepmtVatDiffToDeduct",
			pdl."prepmtVatDiffDeducted",
			pdl."outstandingAmtExVatLcy",
			pdl."aRcdNotInvExVatLcy",
			pdl."dimensionSetId",
			pdl."jobTaskNumber",
			pdl."jobLineType",
			pdl."jobUnitPrice",
			pdl."jobTotalPrice",
			pdl."jobLineAmount",
			pdl."jobLineDiscountAmount",
			pdl."jobLineDiscountPercent",
			pdl."jobUnitPriceLcy",
			pdl."jobTotalPriceLcy",
			pdl."jobLineAmountLcy",
			pdl."jobLineDiscAmountLcy",
			pdl."jobCurrencyFactor",
			pdl."jobCurrencyCode",
			pdl."jobPlanningLineNumber",
			pdl."jobRemainingQty",
			pdl."jobRemainingQtyBase",
			pdl."deferralCode",
			pdl."returnsDeferralStartDate",
			pdl."prodOrderNumber",
			pdl."variantCode",
			pdl."binCode",
			pdl."qtyPerUnitOfMeasure",
			pdl."unitOfMeasureCode",
			pdl."quantityBase",
			pdl."outstandingQtyBase",
			pdl."qtyToInvoiceBase",
			pdl."qtyToReceiveBase",
			pdl."qtyRcdNotInvoicedBase",
			pdl."qtyReceivedBase",
			pdl."qtyInvoicedBase",
			pdl."reservedQtyBase",
			pdl."faPostingDate",
			pdl."faPostingType",
			pdl."depreciationBookCode",
			pdl."salvageValue",
			pdl."deprUntilFaPostingDate",
			pdl."deprAcquisitionCost",
			pdl."maintenanceCode",
			pdl."insuranceNumber",
			pdl."budgetedFaNumber",
			pdl."duplicateInDepreciationBook",
			pdl."useDuplicationList",
			pdl."responsibilityCenter",
			pdl."itemReferenceNumber",
			pdl."itemRefUnitOfMeasure",
			pdl."itemReferenceType",
			pdl."itemReferenceTypeNumber",
			pdl."itemCategoryCode",
			pdl.nonstock,
			pdl."purchasingCode",
			pdl."specialOrder",
			pdl."specialOrderSalesNumber",
			pdl."specialOrderSalesLineNumber",
			pdl."whseOutstandingQtyBase",
			pdl."completelyReceived",
			pdl."requestedReceiptDate",
			pdl."promisedReceiptDate",
			pdl."leadTimeCalculation",
			pdl."inboundWhseHandlingTime",
			pdl."plannedReceiptDate",
			pdl."orderDate",
			pdl."allowItemChargeAssignment",
			pdl."qtyToAssign",
			pdl."qtyAssigned",
			pdl."returnQtyToShip",
			pdl."returnQtyToShipBase",
			pdl."returnQtyShippedNotInvd",
			pdl."retQtyShpdNotInvdBase",
			pdl."returnShpdNotInvd",
			pdl."returnShpdNotInvdLcy",
			pdl."returnQtyShipped",
			pdl."returnQtyShippedBase",
			pdl."returnShipmentNumber",
			pdl."returnShipmentLineNumber",
			pdl."returnReasonCode",
			pdl.subtype,
			pdl."routingNumber",
			pdl."operationNumber",
			pdl."workCenterNumber",
			pdl.finished,
			pdl."prodOrderLineNumber",
			pdl."overheadRate",
			pdl."mpsOrder",
			pdl."planningFlexibility",
			pdl."safetyLeadTime",
			pdl."routingReferenceNumber",
			pdl."SystemModifiedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pdl

    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_purchase_document_lines()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_purchase_document_lines_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Line_Number"
			,"Buy_From_Vendor_No"
			,"Key_Buy_From_Vendor_No"
			,"Type"
			,"No"
			,"Key_Item_No"
			,"Location_Code"
			,"Posting_Group"
			,"Expected_Receipt_Date"
			,"Description"
			,"Description_2"
			,"Unit_Of_Measure"
			,"Quantity"
			,"Outstanding_Quantity"
			,"Qty_To_Invoice"
			,"Qty_To_Receive"
			,"Direct_Unit_Cost"
			,"Unit_Cost_Lcy"
			,"Vat_Percent"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Amount"
			,"Amount_Including_Vat"
			,"Unit_Price_Lcy"
			,"Allow_Invoice_Disc"
			,"Gross_Weight"
			,"Net_Weight"
			,"Units_Per_Parcel"
			,"Unit_Volume"
			,"Appl_To_Item_Entry"
			,"Shortcut_Dimension1_Code"
			,"Shortcut_Dimension2_Code"
			,"Job_Number"
			,"Indirect_Cost_Percent"
			,"Recalculate_Invoice_Disc"
			,"Outstanding_Amount"
			,"Qty_Rcd_Not_Invoiced"
			,"Amt_Rcd_Not_Invoiced"
			,"Quantity_Received"
			,"Quantity_Invoiced"
			,"Receipt_Number"
			,"Receipt_Line_Number"
			,"Profit_Percent"
			,"Pay_To_Vendor_Number"
			,"Inv_Discount_Amount"
			,"Vendor_Item_Number"
			,"Sales_Order_Number"
			,"Sales_Order_Line_Number"
			,"Drop_Shipment"
			,"Gen_Bus_Posting_Group"
			,"Gen_Prod_Posting_Group"
			,"Vat_Calculation_Type"
			,"Transaction_Type"
			,"Transport_Method"
			,"Attached_To_Line_Number"
			,"Entry_Point"
			,"Area"
			,"Transaction_Specification"
			,"Tax_Area_Code"
			,"Tax_Liable"
			,"Tax_Group_Code"
			,"Use_Tax"
			,"Vat_Bus_Posting_Group"
			,"Vat_Prod_Posting_Group"
			,"Currency_Code"
			,"Outstanding_Amount_Lcy"
			,"Amt_Rcd_Not_Invoiced_Lcy"
			,"Reserved_Quantity"
			,"Blanket_Order_Number"
			,"Blanket_Order_Line_Number"
			,"Vat_Base_Amount"
			,"Unit_Cost"
			,"System_Created_Entry"
			,"Line_Amount"
			,"Vat_Difference"
			,"Inv_Disc_Amount_To_Invoice"
			,"Vat_Identifier"
			,"Ic_Partner_Ref_Type"
			,"Ic_Partner_Reference"
			,"Prepayment_Percent"
			,"Prepmt_Line_Amount"
			,"Prepmt_Amt_Inv"
			,"Prepmt_Amt_Incl_Vat"
			,"Prepayment_Amount"
			,"Prepmt_Vat_Base_Amt"
			,"Prepayment_Vat_Percent"
			,"Prepmt_Vat_Calc_Type"
			,"Prepayment_Vat_Identifier"
			,"Prepayment_Tax_Area_Code"
			,"Prepayment_Tax_Liable"
			,"Prepayment_Tax_Group_Code"
			,"Prepmt_Amt_To_Deduct"
			,"Prepmt_Amt_Deducted"
			,"Prepayment_Line"
			,"Prepmt_Amount_Inv_Incl_Vat"
			,"Prepmt_Amount_Inv_Lcy"
			,"Ic_Partner_Code"
			,"Prepmt_Vat_Amount_Inv_Lcy"
			,"Prepayment_Vat_Difference"
			,"Prepmt_Vat_Diff_To_Deduct"
			,"Prepmt_Vat_Diff_Deducted"
			,"Outstanding_Amt_Ex_Vat_Lcy"
			,"ARcd_Not_Inv_Ex_Vat_Lcy"
			,"Dimension_Set_Id"
			,"Job_Task_Number"
			,"Job_Line_Type"
			,"Job_Unit_Price"
			,"Job_Total_Price"
			,"Job_Line_Amount"
			,"Job_Line_Discount_Amount"
			,"Job_Line_Discount_Percent"
			,"Job_Unit_Price_Lcy"
			,"Job_Total_Price_Lcy"
			,"Job_Line_Amount_Lcy"
			,"Job_Line_Disc_Amount_Lcy"
			,"Job_Currency_Factor"
			,"Job_Currency_Code"
			,"Job_Planning_Line_Number"
			,"Job_Remaining_Qty"
			,"Job_Remaining_Qty_Base"
			,"Deferral_Code"
			,"Returns_Deferral_Start_Date"
			,"Prod_Order_Number"
			,"Variant_Code"
			,"Bin_Code"
			,"Qty_Per_Unit_Of_Measure"
			,"Unit_Of_Measure_Code"
			,"Quantity_Base"
			,"Outstanding_Qty_Base"
			,"Qty_To_Invoice_Base"
			,"Qty_To_Receive_Base"
			,"Qty_Rcd_Not_Invoiced_Base"
			,"Qty_Received_Base"
			,"Qty_Invoiced_Base"
			,"Reserved_Qty_Base"
			,"Fa_Posting_Date"
			,"Fa_Posting_Type"
			,"Depreciation_Book_Code"
			,"Salvage_Value"
			,"Depr_Until_Fa_Posting_Date"
			,"Depr_Acquisition_Cost"
			,"Maintenance_Code"
			,"Insurance_Number"
			,"Budgeted_Fa_Number"
			,"Duplicate_In_Depreciation_Book"
			,"Use_Duplication_List"
			,"Responsibility_Center"
			,"Item_Reference_Number"
			,"Item_Ref_Unit_Of_Measure"
			,"Item_Reference_Type"
			,"Item_Reference_Type_Number"
			,"Item_Category_Code"
			,"Nonstock"
			,"Purchasing_Code"
			,"Special_Order"
			,"Special_Order_Sales_Number"
			,"Special_Order_Sales_Line_Number"
			,"Whse_Outstanding_Qty_Base"
			,"Completely_Received"
			,"Requested_Receipt_Date"
			,"Promised_Receipt_Date"
			,"Lead_Time_Calculation"
			,"Inbound_Whse_Handling_Time"
			,"Planned_Receipt_Date"
			,"Order_Date"
			,"Allow_Item_Charge_Assignment"
			,"Qty_To_Assign"
			,"Qty_Assigned"
			,"Return_Qty_To_Ship"
			,"Return_Qty_To_Ship_Base"
			,"Return_Qty_Shipped_Not_Invd"
			,"Ret_Qty_Shpd_Not_Invd_Base"
			,"Return_Shpd_Not_Invd"
			,"Return_Shpd_Not_Invd_Lcy"
			,"Return_Qty_Shipped"
			,"Return_Qty_Shipped_Base"
			,"Return_Shipment_Number"
			,"Return_Shipment_Line_Number"
			,"Return_Reason_Code"
			,"Subtype"
			,"Routing_Number"
			,"Operation_Number"
			,"Work_Center_Number"
			,"Finished"
			,"Prod_Order_Line_Number"
			,"Overhead_Rate"
			,"Mps_Order"
			,"Planning_Flexibility"
			,"Safety_Lead_Time"
			,"Routing_Reference_Number"
			,"System_Modified_At"
			,"Firma"
			,"load_ts"
	)
	SELECT 
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
		$31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
		$41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
		$51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
		$61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
		$71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
		$81, $82, $83, $84, $85, $86, $87, $88, $89, $90,
		$91, $92, $93, $94, $95, $96, $97, $98, $99, $100,
		$101, $102, $103, $104, $105, $106, $107, $108, $109, $110,
		$111, $112, $113, $114, $115, $116, $117, $118, $119, $120,
		$121, $122, $123, $124, $125, $126, $127, $128, $129, $130,
		$131, $132, $133, $134, $135, $136, $137, $138, $139, $140,
		$141, $142, $143, $144, $145, $146, $147, $148, $149, $150,
		$151, $152, $153, $154, $155, $156, $157, $158, $159, $160,
		$161, $162, $163, $164, $165, $166, $167, $168, $169, $170,
		$171, $172, $173, $174, $175, $176, $177, $178, $179, $180,
		$181, $182, $183, $184, $185, $186, $187, $188, $189, $190,
		$191, $192, $193, $194, $195, $196
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Line_Number", "Document_Number", "Document_Type") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"Document_No" = EXCLUDED."Document_No"
		,"Key_Document_No" = EXCLUDED."Key_Document_No"
		,"Line_Number" = EXCLUDED."Line_Number"
		,"Buy_From_Vendor_No" = EXCLUDED."Buy_From_Vendor_No"
		,"Key_Buy_From_Vendor_No" = EXCLUDED."Key_Buy_From_Vendor_No"
		,"Type" = EXCLUDED."Type"
		,"No" = EXCLUDED."No"
		,"Key_Item_No" = EXCLUDED."Key_Item_No"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Posting_Group" = EXCLUDED."Posting_Group"
		,"Expected_Receipt_Date" = EXCLUDED."Expected_Receipt_Date"
		,"Description" = EXCLUDED."Description"
		,"Description_2" = EXCLUDED."Description_2"
		,"Unit_Of_Measure" = EXCLUDED."Unit_Of_Measure"
		,"Quantity" = EXCLUDED."Quantity"
		,"Outstanding_Quantity" = EXCLUDED."Outstanding_Quantity"
		,"Qty_To_Invoice" = EXCLUDED."Qty_To_Invoice"
		,"Qty_To_Receive" = EXCLUDED."Qty_To_Receive"
		,"Direct_Unit_Cost" = EXCLUDED."Direct_Unit_Cost"
		,"Unit_Cost_Lcy" = EXCLUDED."Unit_Cost_Lcy"
		,"Vat_Percent" = EXCLUDED."Vat_Percent"
		,"Line_Discount_Percent" = EXCLUDED."Line_Discount_Percent"
		,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
		,"Amount" = EXCLUDED."Amount"
		,"Amount_Including_Vat" = EXCLUDED."Amount_Including_Vat"
		,"Unit_Price_Lcy" = EXCLUDED."Unit_Price_Lcy"
		,"Allow_Invoice_Disc" = EXCLUDED."Allow_Invoice_Disc"
		,"Gross_Weight" = EXCLUDED."Gross_Weight"
		,"Net_Weight" = EXCLUDED."Net_Weight"
		,"Units_Per_Parcel" = EXCLUDED."Units_Per_Parcel"
		,"Unit_Volume" = EXCLUDED."Unit_Volume"
		,"Appl_To_Item_Entry" = EXCLUDED."Appl_To_Item_Entry"
		,"Shortcut_Dimension1_Code" = EXCLUDED."Shortcut_Dimension1_Code"
		,"Shortcut_Dimension2_Code" = EXCLUDED."Shortcut_Dimension2_Code"
		,"Job_Number" = EXCLUDED."Job_Number"
		,"Indirect_Cost_Percent" = EXCLUDED."Indirect_Cost_Percent"
		,"Recalculate_Invoice_Disc" = EXCLUDED."Recalculate_Invoice_Disc"
		,"Outstanding_Amount" = EXCLUDED."Outstanding_Amount"
		,"Qty_Rcd_Not_Invoiced" = EXCLUDED."Qty_Rcd_Not_Invoiced"
		,"Amt_Rcd_Not_Invoiced" = EXCLUDED."Amt_Rcd_Not_Invoiced"
		,"Quantity_Received" = EXCLUDED."Quantity_Received"
		,"Quantity_Invoiced" = EXCLUDED."Quantity_Invoiced"
		,"Receipt_Number" = EXCLUDED."Receipt_Number"
		,"Receipt_Line_Number" = EXCLUDED."Receipt_Line_Number"
		,"Profit_Percent" = EXCLUDED."Profit_Percent"
		,"Pay_To_Vendor_Number" = EXCLUDED."Pay_To_Vendor_Number"
		,"Inv_Discount_Amount" = EXCLUDED."Inv_Discount_Amount"
		,"Vendor_Item_Number" = EXCLUDED."Vendor_Item_Number"
		,"Sales_Order_Number" = EXCLUDED."Sales_Order_Number"
		,"Sales_Order_Line_Number" = EXCLUDED."Sales_Order_Line_Number"
		,"Drop_Shipment" = EXCLUDED."Drop_Shipment"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Gen_Prod_Posting_Group" = EXCLUDED."Gen_Prod_Posting_Group"
		,"Vat_Calculation_Type" = EXCLUDED."Vat_Calculation_Type"
		,"Transaction_Type" = EXCLUDED."Transaction_Type"
		,"Transport_Method" = EXCLUDED."Transport_Method"
		,"Attached_To_Line_Number" = EXCLUDED."Attached_To_Line_Number"
		,"Entry_Point" = EXCLUDED."Entry_Point"
		,"Area" = EXCLUDED."Area"
		,"Transaction_Specification" = EXCLUDED."Transaction_Specification"
		,"Tax_Area_Code" = EXCLUDED."Tax_Area_Code"
		,"Tax_Liable" = EXCLUDED."Tax_Liable"
		,"Tax_Group_Code" = EXCLUDED."Tax_Group_Code"
		,"Use_Tax" = EXCLUDED."Use_Tax"
		,"Vat_Bus_Posting_Group" = EXCLUDED."Vat_Bus_Posting_Group"
		,"Vat_Prod_Posting_Group" = EXCLUDED."Vat_Prod_Posting_Group"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Outstanding_Amount_Lcy" = EXCLUDED."Outstanding_Amount_Lcy"
		,"Amt_Rcd_Not_Invoiced_Lcy" = EXCLUDED."Amt_Rcd_Not_Invoiced_Lcy"
		,"Reserved_Quantity" = EXCLUDED."Reserved_Quantity"
		,"Blanket_Order_Number" = EXCLUDED."Blanket_Order_Number"
		,"Blanket_Order_Line_Number" = EXCLUDED."Blanket_Order_Line_Number"
		,"Vat_Base_Amount" = EXCLUDED."Vat_Base_Amount"
		,"Unit_Cost" = EXCLUDED."Unit_Cost"
		,"System_Created_Entry" = EXCLUDED."System_Created_Entry"
		,"Line_Amount" = EXCLUDED."Line_Amount"
		,"Vat_Difference" = EXCLUDED."Vat_Difference"
		,"Inv_Disc_Amount_To_Invoice" = EXCLUDED."Inv_Disc_Amount_To_Invoice"
		,"Vat_Identifier" = EXCLUDED."Vat_Identifier"
		,"Ic_Partner_Ref_Type" = EXCLUDED."Ic_Partner_Ref_Type"
		,"Ic_Partner_Reference" = EXCLUDED."Ic_Partner_Reference"
		,"Prepayment_Percent" = EXCLUDED."Prepayment_Percent"
		,"Prepmt_Line_Amount" = EXCLUDED."Prepmt_Line_Amount"
		,"Prepmt_Amt_Inv" = EXCLUDED."Prepmt_Amt_Inv"
		,"Prepmt_Amt_Incl_Vat" = EXCLUDED."Prepmt_Amt_Incl_Vat"
		,"Prepayment_Amount" = EXCLUDED."Prepayment_Amount"
		,"Prepmt_Vat_Base_Amt" = EXCLUDED."Prepmt_Vat_Base_Amt"
		,"Prepayment_Vat_Percent" = EXCLUDED."Prepayment_Vat_Percent"
		,"Prepmt_Vat_Calc_Type" = EXCLUDED."Prepmt_Vat_Calc_Type"
		,"Prepayment_Vat_Identifier" = EXCLUDED."Prepayment_Vat_Identifier"
		,"Prepayment_Tax_Area_Code" = EXCLUDED."Prepayment_Tax_Area_Code"
		,"Prepayment_Tax_Liable" = EXCLUDED."Prepayment_Tax_Liable"
		,"Prepayment_Tax_Group_Code" = EXCLUDED."Prepayment_Tax_Group_Code"
		,"Prepmt_Amt_To_Deduct" = EXCLUDED."Prepmt_Amt_To_Deduct"
		,"Prepmt_Amt_Deducted" = EXCLUDED."Prepmt_Amt_Deducted"
		,"Prepayment_Line" = EXCLUDED."Prepayment_Line"
		,"Prepmt_Amount_Inv_Incl_Vat" = EXCLUDED."Prepmt_Amount_Inv_Incl_Vat"
		,"Prepmt_Amount_Inv_Lcy" = EXCLUDED."Prepmt_Amount_Inv_Lcy"
		,"Ic_Partner_Code" = EXCLUDED."Ic_Partner_Code"
		,"Prepmt_Vat_Amount_Inv_Lcy" = EXCLUDED."Prepmt_Vat_Amount_Inv_Lcy"
		,"Prepayment_Vat_Difference" = EXCLUDED."Prepayment_Vat_Difference"
		,"Prepmt_Vat_Diff_To_Deduct" = EXCLUDED."Prepmt_Vat_Diff_To_Deduct"
		,"Prepmt_Vat_Diff_Deducted" = EXCLUDED."Prepmt_Vat_Diff_Deducted"
		,"Outstanding_Amt_Ex_Vat_Lcy" = EXCLUDED."Outstanding_Amt_Ex_Vat_Lcy"
		,"ARcd_Not_Inv_Ex_Vat_Lcy" = EXCLUDED."ARcd_Not_Inv_Ex_Vat_Lcy"
		,"Dimension_Set_Id" = EXCLUDED."Dimension_Set_Id"
		,"Job_Task_Number" = EXCLUDED."Job_Task_Number"
		,"Job_Line_Type" = EXCLUDED."Job_Line_Type"
		,"Job_Unit_Price" = EXCLUDED."Job_Unit_Price"
		,"Job_Total_Price" = EXCLUDED."Job_Total_Price"
		,"Job_Line_Amount" = EXCLUDED."Job_Line_Amount"
		,"Job_Line_Discount_Amount" = EXCLUDED."Job_Line_Discount_Amount"
		,"Job_Line_Discount_Percent" = EXCLUDED."Job_Line_Discount_Percent"
		,"Job_Unit_Price_Lcy" = EXCLUDED."Job_Unit_Price_Lcy"
		,"Job_Total_Price_Lcy" = EXCLUDED."Job_Total_Price_Lcy"
		,"Job_Line_Amount_Lcy" = EXCLUDED."Job_Line_Amount_Lcy"
		,"Job_Line_Disc_Amount_Lcy" = EXCLUDED."Job_Line_Disc_Amount_Lcy"
		,"Job_Currency_Factor" = EXCLUDED."Job_Currency_Factor"
		,"Job_Currency_Code" = EXCLUDED."Job_Currency_Code"
		,"Job_Planning_Line_Number" = EXCLUDED."Job_Planning_Line_Number"
		,"Job_Remaining_Qty" = EXCLUDED."Job_Remaining_Qty"
		,"Job_Remaining_Qty_Base" = EXCLUDED."Job_Remaining_Qty_Base"
		,"Deferral_Code" = EXCLUDED."Deferral_Code"
		,"Returns_Deferral_Start_Date" = EXCLUDED."Returns_Deferral_Start_Date"
		,"Prod_Order_Number" = EXCLUDED."Prod_Order_Number"
		,"Variant_Code" = EXCLUDED."Variant_Code"
		,"Bin_Code" = EXCLUDED."Bin_Code"
		,"Qty_Per_Unit_Of_Measure" = EXCLUDED."Qty_Per_Unit_Of_Measure"
		,"Unit_Of_Measure_Code" = EXCLUDED."Unit_Of_Measure_Code"
		,"Quantity_Base" = EXCLUDED."Quantity_Base"
		,"Outstanding_Qty_Base" = EXCLUDED."Outstanding_Qty_Base"
		,"Qty_To_Invoice_Base" = EXCLUDED."Qty_To_Invoice_Base"
		,"Qty_To_Receive_Base" = EXCLUDED."Qty_To_Receive_Base"
		,"Qty_Rcd_Not_Invoiced_Base" = EXCLUDED."Qty_Rcd_Not_Invoiced_Base"
		,"Qty_Received_Base" = EXCLUDED."Qty_Received_Base"
		,"Qty_Invoiced_Base" = EXCLUDED."Qty_Invoiced_Base"
		,"Reserved_Qty_Base" = EXCLUDED."Reserved_Qty_Base"
		,"Fa_Posting_Date" = EXCLUDED."Fa_Posting_Date"
		,"Fa_Posting_Type" = EXCLUDED."Fa_Posting_Type"
		,"Depreciation_Book_Code" = EXCLUDED."Depreciation_Book_Code"
		,"Salvage_Value" = EXCLUDED."Salvage_Value"
		,"Depr_Until_Fa_Posting_Date" = EXCLUDED."Depr_Until_Fa_Posting_Date"
		,"Depr_Acquisition_Cost" = EXCLUDED."Depr_Acquisition_Cost"
		,"Maintenance_Code" = EXCLUDED."Maintenance_Code"
		,"Insurance_Number" = EXCLUDED."Insurance_Number"
		,"Budgeted_Fa_Number" = EXCLUDED."Budgeted_Fa_Number"
		,"Duplicate_In_Depreciation_Book" = EXCLUDED."Duplicate_In_Depreciation_Book"
		,"Use_Duplication_List" = EXCLUDED."Use_Duplication_List"
		,"Responsibility_Center" = EXCLUDED."Responsibility_Center"
		,"Item_Reference_Number" = EXCLUDED."Item_Reference_Number"
		,"Item_Ref_Unit_Of_Measure" = EXCLUDED."Item_Ref_Unit_Of_Measure"
		,"Item_Reference_Type" = EXCLUDED."Item_Reference_Type"
		,"Item_Reference_Type_Number" = EXCLUDED."Item_Reference_Type_Number"
		,"Item_Category_Code" = EXCLUDED."Item_Category_Code"
		,"Nonstock" = EXCLUDED."Nonstock"
		,"Purchasing_Code" = EXCLUDED."Purchasing_Code"
		,"Special_Order" = EXCLUDED."Special_Order"
		,"Special_Order_Sales_Number" = EXCLUDED."Special_Order_Sales_Number"
		,"Special_Order_Sales_Line_Number" = EXCLUDED."Special_Order_Sales_Line_Number"
		,"Whse_Outstanding_Qty_Base" = EXCLUDED."Whse_Outstanding_Qty_Base"
		,"Completely_Received" = EXCLUDED."Completely_Received"
		,"Requested_Receipt_Date" = EXCLUDED."Requested_Receipt_Date"
		,"Promised_Receipt_Date" = EXCLUDED."Promised_Receipt_Date"
		,"Lead_Time_Calculation" = EXCLUDED."Lead_Time_Calculation"
		,"Inbound_Whse_Handling_Time" = EXCLUDED."Inbound_Whse_Handling_Time"
		,"Planned_Receipt_Date" = EXCLUDED."Planned_Receipt_Date"
		,"Order_Date" = EXCLUDED."Order_Date"
		,"Allow_Item_Charge_Assignment" = EXCLUDED."Allow_Item_Charge_Assignment"
		,"Qty_To_Assign" = EXCLUDED."Qty_To_Assign"
		,"Qty_Assigned" = EXCLUDED."Qty_Assigned"
		,"Return_Qty_To_Ship" = EXCLUDED."Return_Qty_To_Ship"
		,"Return_Qty_To_Ship_Base" = EXCLUDED."Return_Qty_To_Ship_Base"
		,"Return_Qty_Shipped_Not_Invd" = EXCLUDED."Return_Qty_Shipped_Not_Invd"
		,"Ret_Qty_Shpd_Not_Invd_Base" = EXCLUDED."Ret_Qty_Shpd_Not_Invd_Base"
		,"Return_Shpd_Not_Invd" = EXCLUDED."Return_Shpd_Not_Invd"
		,"Return_Shpd_Not_Invd_Lcy" = EXCLUDED."Return_Shpd_Not_Invd_Lcy"
		,"Return_Qty_Shipped" = EXCLUDED."Return_Qty_Shipped"
		,"Return_Qty_Shipped_Base" = EXCLUDED."Return_Qty_Shipped_Base"
		,"Return_Shipment_Number" = EXCLUDED."Return_Shipment_Number"
		,"Return_Shipment_Line_Number" = EXCLUDED."Return_Shipment_Line_Number"
		,"Return_Reason_Code" = EXCLUDED."Return_Reason_Code"
		,"Subtype" = EXCLUDED."Subtype"
		,"Routing_Number" = EXCLUDED."Routing_Number"
		,"Operation_Number" = EXCLUDED."Operation_Number"
		,"Work_Center_Number" = EXCLUDED."Work_Center_Number"
		,"Finished" = EXCLUDED."Finished"
		,"Prod_Order_Line_Number" = EXCLUDED."Prod_Order_Line_Number"
		,"Overhead_Rate" = EXCLUDED."Overhead_Rate"
		,"Mps_Order" = EXCLUDED."Mps_Order"
		,"Planning_Flexibility" = EXCLUDED."Planning_Flexibility"
		,"Safety_Lead_Time" = EXCLUDED."Safety_Lead_Time"
		,"Routing_Reference_Number" = EXCLUDED."Routing_Reference_Number"
		,"System_Modified_At" = EXCLUDED."System_Modified_At"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."documentType"
		,NEW."documentNumber"
		,CONCAT(litera_firmy, '_', NEW."documentNumber")
		,NEW."lineNumber"
		,NEW."buyFromVendorNumber"
		,CONCAT(litera_firmy, '_', NEW."buyFromVendorNumber")
		,NEW."type"
		,NEW."number"
		,CONCAT(litera_firmy, '_', NEW."number")
		,NEW."locationCode"
		,NEW."postingGroup"
		,NEW."expectedReceiptDate"
		,NEW."description"
		,NEW."description2"
		,NEW."unitOfMeasure"
		,NEW."quantity"
		,NEW."outstandingQuantity"
		,NEW."qtyToInvoice"
		,NEW."qtyToReceive"
		,NEW."directUnitCost"
		,NEW."unitCostLcy"
		,NEW."vatPercent"
		,NEW."lineDiscountPercent"
		,NEW."lineDiscountAmount"
		,NEW.amount
		,NEW."amountIncludingVat"
		,NEW."unitPriceLcy"
		,NEW."allowInvoiceDisc"
		,NEW."grossWeight"
		,NEW."netWeight"
		,NEW."unitsPerParcel"
		,NEW."unitVolume"
		,NEW."applToItemEntry"
		,NEW."shortcutDimension1Code"
		,NEW."shortcutDimension2Code"
		,NEW."jobNumber"
		,NEW."indirectCostPercent"
		,NEW."recalculateInvoiceDisc"
		,NEW."outstandingAmount"
		,NEW."qtyRcdNotInvoiced"
		,NEW."amtRcdNotInvoiced"
		,NEW."quantityReceived"
		,NEW."quantityInvoiced"
		,NEW."receiptNumber"
		,NEW."receiptLineNumber"
		,NEW."profitPercent"
		,NEW."payToVendorNumber"
		,NEW."invDiscountAmount"
		,NEW."vendorItemNumber"
		,NEW."salesOrderNumber"
		,NEW."salesOrderLineNumber"
		,NEW."dropShipment"
		,NEW."genBusPostingGroup"
		,NEW."genProdPostingGroup"
		,NEW."vatCalculationType"
		,NEW."transactionType"
		,NEW."transportMethod"
		,NEW."attachedToLineNumber"
		,NEW."entryPoint"
		,NEW.area
		,NEW."transactionSpecification"
		,NEW."taxAreaCode"
		,NEW."taxLiable"
		,NEW."taxGroupCode"
		,NEW."useTax"
		,NEW."vatBusPostingGroup"
		,NEW."vatProdPostingGroup"
		,NEW."currencyCode"
		,NEW."outstandingAmountLcy"
		,NEW."amtRcdNotInvoicedLcy"
		,NEW."reservedQuantity"
		,NEW."blanketOrderNumber"
		,NEW."blanketOrderLineNumber"
		,NEW."vatBaseAmount"
		,NEW."unitCost"
		,NEW."systemCreatedEntry"
		,NEW."lineAmount"
		,NEW."vatDifference"
		,NEW."invDiscAmountToInvoice"
		,NEW."vatIdentifier"
		,NEW."icPartnerRefType"
		,NEW."icPartnerReference"
		,NEW."prepaymentPercent"
		,NEW."prepmtLineAmount"
		,NEW."prepmtAmtInv"
		,NEW."prepmtAmtInclVat"
		,NEW."prepaymentAmount"
		,NEW."prepmtVatBaseAmt"
		,NEW."prepaymentVatPercent"
		,NEW."prepmtVatCalcType"
		,NEW."prepaymentVatIdentifier"
		,NEW."prepaymentTaxAreaCode"
		,NEW."prepaymentTaxLiable"
		,NEW."prepaymentTaxGroupCode"
		,NEW."prepmtAmtToDeduct"
		,NEW."prepmtAmtDeducted"
		,NEW."prepaymentLine"
		,NEW."prepmtAmountInvInclVat"
		,NEW."prepmtAmountInvLcy"
		,NEW."icPartnerCode"
		,NEW."prepmtVatAmountInvLcy"
		,NEW."prepaymentVatDifference"
		,NEW."prepmtVatDiffToDeduct"
		,NEW."prepmtVatDiffDeducted"
		,NEW."outstandingAmtExVatLcy"
		,NEW."aRcdNotInvExVatLcy"
		,NEW."dimensionSetId"
		,NEW."jobTaskNumber"
		,NEW."jobLineType"
		,NEW."jobUnitPrice"
		,NEW."jobTotalPrice"
		,NEW."jobLineAmount"
		,NEW."jobLineDiscountAmount"
		,NEW."jobLineDiscountPercent"
		,NEW."jobUnitPriceLcy"
		,NEW."jobTotalPriceLcy"
		,NEW."jobLineAmountLcy"
		,NEW."jobLineDiscAmountLcy"
		,NEW."jobCurrencyFactor"
		,NEW."jobCurrencyCode"
		,NEW."jobPlanningLineNumber"
		,NEW."jobRemainingQty"
		,NEW."jobRemainingQtyBase"
		,NEW."deferralCode"
		,NEW."returnsDeferralStartDate"
		,NEW."prodOrderNumber"
		,NEW."variantCode"
		,NEW."binCode"
		,NEW."qtyPerUnitOfMeasure"
		,NEW."unitOfMeasureCode"
		,NEW."quantityBase"
		,NEW."outstandingQtyBase"
		,NEW."qtyToInvoiceBase"
		,NEW."qtyToReceiveBase"
		,NEW."qtyRcdNotInvoicedBase"
		,NEW."qtyReceivedBase"
		,NEW."qtyInvoicedBase"
		,NEW."reservedQtyBase"
		,NEW."faPostingDate"
		,NEW."faPostingType"
		,NEW."depreciationBookCode"
		,NEW."salvageValue"
		,NEW."deprUntilFaPostingDate"
		,NEW."deprAcquisitionCost"
		,NEW."maintenanceCode"
		,NEW."insuranceNumber"
		,NEW."budgetedFaNumber"
		,NEW."duplicateInDepreciationBook"
		,NEW."useDuplicationList"
		,NEW."responsibilityCenter"
		,NEW."itemReferenceNumber"
		,NEW."itemRefUnitOfMeasure"
		,NEW."itemReferenceType"
		,NEW."itemReferenceTypeNumber"
		,NEW."itemCategoryCode"
		,NEW.nonstock
		,NEW."purchasingCode"
		,NEW."specialOrder"
		,NEW."specialOrderSalesNumber"
		,NEW."specialOrderSalesLineNumber"
		,NEW."whseOutstandingQtyBase"
		,NEW."completelyReceived"
		,NEW."requestedReceiptDate"
		,NEW."promisedReceiptDate"
		,NEW."leadTimeCalculation"
		,NEW."inboundWhseHandlingTime"
		,NEW."plannedReceiptDate"
		,NEW."orderDate"
		,NEW."allowItemChargeAssignment"
		,NEW."qtyToAssign"
		,NEW."qtyAssigned"
		,NEW."returnQtyToShip"
		,NEW."returnQtyToShipBase"
		,NEW."returnQtyShippedNotInvd"
		,NEW."retQtyShpdNotInvdBase"
		,NEW."returnShpdNotInvd"
		,NEW."returnShpdNotInvdLcy"
		,NEW."returnQtyShipped"
		,NEW."returnQtyShippedBase"
		,NEW."returnShipmentNumber"
		,NEW."returnShipmentLineNumber"
		,NEW."returnReasonCode"
		,NEW.subtype
		,NEW."routingNumber"
		,NEW."operationNumber"
		,NEW."workCenterNumber"
		,NEW.finished
		,NEW."prodOrderLineNumber"
		,NEW."overheadRate"
		,NEW."mpsOrder"
		,NEW."planningFlexibility"
		,NEW."safetyLeadTime"
		,NEW."routingReferenceNumber"
		,NEW."SystemModifiedAt"
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
	grupa_tabel text := 'purchase_document_lines'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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