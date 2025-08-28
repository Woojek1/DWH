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
	_tabela := format('bc_purchase_documents_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---
					 
-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NOT NULL
			,"Buy_From_Vendor_Number" text NULL
			,"Key_Buy_From_Vendor_Number" text NULL
			,"Number" text NOT NULL
			,"Key_Number" text NOT NULL
			,"Pay_To_Vendor_Number" text NULL
			,"Pay_To_Name" text NULL
			,"Pay_To_Name2" text NULL
			,"Pay_To_Address" text NULL
			,"Pay_To_Address2" text NULL
			,"Pay_To_City" text NULL
			,"Pay_To_Contact" text NULL
			,"Your_Reference" text NULL
			,"Ship_To_Code" text NULL
			,"Ship_To_Name" text NULL
			,"Ship_To_Name2" text NULL
			,"Ship_To_Address" text NULL
			,"Ship_To_Address2" text NULL
			,"Ship_To_City" text NULL
			,"Ship_To_Contact" text NULL
			,"Order_Date" date NULL
			,"Posting_Date" date NULL
			,"Expected_Receipt_Date" date NULL
			,"Posting_Description" text NULL
			,"Payment_Terms_Code" text NULL
			,"Due_Date" date NULL
			,"Payment_Discount_Percent" numeric(6, 2) NULL
			,"Pmt_Discount_Date" date NULL
			,"Shipment_Method_Code" text NULL
			,"Location_Code" text NULL
			,"Shortcut_Dimension1_Code" text NULL
			,"Shortcut_Dimension2_Code" text NULL
			,"Vendor_Posting_Group" text NULL
			,"Currency_Code" text NULL
			,"Currency_Factor" numeric(14, 2) NULL
			,"Prices_Including_VAT" bool NULL
			,"Invoice_Disc_Code" text NULL
			,"Language_Code" text NULL
			,"Purchaser_Code" text NULL
			,"Order_Class" text NULL
			,"Comment" bool NULL
			,"Number_Printed" int4 NULL
			,"On_Hold" text NULL
			,"Applies_To_Doc_Type" text NULL
			,"Applies_To_Doc_Number" text NULL
			,"Bal_Account_Number" text NULL
			,"Recalculate_Invoice_Disc" bool NULL
			,"Receive" bool NULL
			,"Invoice" bool NULL
			,"Print_Posted_Documents" bool NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_Including_VAT" numeric(14, 2) NULL
			,"Receiving_Number" text NULL
			,"Posting_Number" text NULL
			,"Last_Receiving_Number" text NULL
			,"Last_Posting_Number" text NULL
			,"Vendor_Order_Number" text NULL
			,"Vendor_Shipment_Number" text NULL
			,"Vendor_Invoice_Number" text NULL
			,"Vendor_Cr_Memo_Number" text NULL
			,"VAT_Registration_Number" text NULL
			,"Sell_To_Customer_Number" text NULL
			,"Reason_Code" text NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Transaction_Type" text NULL
			,"Transport_Method" text NULL
			,"VAT_Country_Region_Code" text NULL
			,"Buy_From_Vendor_Name" text NULL
			,"Buy_From_Vendor_Name2" text NULL
			,"Buy_From_Address" text NULL
			,"Buy_From_Address2" text NULL
			,"Buy_From_City" text NULL
			,"Buy_From_Contact" text NULL
			,"Pay_To_Post_Code" text NULL
			,"Pay_To_County" text NULL
			,"Pay_To_Country_Region_Code" text NULL
			,"Buy_From_Post_Code" text NULL
			,"Buy_From_County" text NULL
			,"Buy_From_Country_Region_Code" text NULL
			,"Ship_To_Post_Code" text NULL
			,"Ship_To_County" text NULL
			,"Ship_To_Country_Region_Code" text NULL
			,"Bal_Account_Type" text NULL
			,"Order_Address_Code" text NULL
			,"Entry_Point" text NULL
			,"Correction" bool NULL
			,"Document_Date" date NULL
			,"Area" text NULL
			,"Transaction_Specification" text NULL
			,"Payment_Method_Code" text NULL
			,"Number_Series" text NULL
			,"Posting_Number_Series" text NULL
			,"Receiving_Number_Series" text NULL
			,"Tax_Area_Code" text NULL
			,"Tax_Liable" bool NULL
			,"VAT_Bus_Posting_Group" text NULL
			,"Applies_To_Id" text NULL
			,"VAT_Base_Discount_Percent" numeric(6, 2) NULL
			,"Status" text NULL
			,"Invoice_Discount_Calculation" text NULL
			,"Invoice_Discount_Value" numeric(14, 2) NULL
			,"Send_IC_Document" bool NULL
			,"IC_Status" text NULL
			,"Buy_From_IC_Partner_Code" text NULL
			,"Pay_To_IC_Partner_Code" text NULL 
			,"IC_Direction" text NULL
			,"Prepayment_Number" text NULL
			,"Last_Prepayment_Number" text NULL
			,"Prepmt_Cr_Memo_Number" text NULL
			,"Last_Prepmt_Cr_Memo_Number" text NULL
			,"Prepayment_Percent" numeric(6, 2) NULL
			,"Prepayment_Number_Series" text NULL
			,"Compress_Prepayment" bool NULL
			,"Prepayment_Due_Date" date NULL
			,"Prepmt_Cr_Memo_Number_Series" text NULL
			,"Prepmt_Posting_Description" text NULL
			,"Prepmt_Pmt_Discount_Date" date NULL
			,"Prepmt_Payment_Terms_Code" text NULL
			,"Prepmt_Payment_Discount_Percent" numeric(6, 2) NULL
			,"Quote_Number" text NULL
			,"Job_Queue_Status" text NULL
			,"Job_Queue_Entry_Id" text NULL
			,"Incoming_Document_Entry_Number" int4 NULL
			,"Creditor_Number" text NULL
			,"Payment_Reference" text NULL
			,"A_Rcd_Not_Inv_Ex_Vat_LCY" numeric(14, 2) NULL
			,"Amt_Rcd_Not_Invoiced_LCY" numeric(14, 2) NULL
			,"Dimension_Set_Id" int4 NULL
			,"Invoice_Discount_Amount" numeric(14, 2) NULL
			,"Number_Of_Archived_Versions" int4 NULL
			,"Doc_Number_Occurrence" int4 NULL
			,"Campaign_Number" text NULL
			,"Buy_From_Contact_Number" text NULL
			,"Pay_To_Contact_Number" text NULL
			,"Responsibility_Center" text NULL
			,"Completely_Received" bool NULL
			,"Posting_From_Whse_Ref" int4 NULL
			,"Location_Filter" text NULL
			,"Requested_Receipt_Date" date NULL
			,"Promised_Receipt_Date" date NULL
			,"Lead_Time_Calculation" text NULL
			,"Inbound_Whse_Handling_Time" text NULL
			,"Vendor_Authorization_Number" text NULL
			,"Return_Shipment_Number" text NULL
			,"Return_Shipment_Number_Series" text NULL
			,"Ship" bool NULL
			,"Last_Return_Shipment_Number" text NULL
			,"Assigned_User_Id" text NULL
			,"Pending_Approvals" int4 NULL
			,"System_Modified_At" timestamptz NULL
			,"Firma" char (1)
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("Document_Type", "Number")
	)
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"Buy_From_Vendor_Number"
			,"Key_Buy_From_Vendor_Number"
			,"Number"
			,"Key_Number"
			,"Pay_To_Vendor_Number"
			,"Pay_To_Name"
			,"Pay_To_Name2"
			,"Pay_To_Address"
			,"Pay_To_Address2"
			,"Pay_To_City"
			,"Pay_To_Contact"
			,"Your_Reference"
			,"Ship_To_Code"
			,"Ship_To_Name"
			,"Ship_To_Name2"
			,"Ship_To_Address"
			,"Ship_To_Address2"
			,"Ship_To_City"
			,"Ship_To_Contact"
			,"Order_Date"
			,"Posting_Date"
			,"Expected_Receipt_Date"
			,"Posting_Description"
			,"Payment_Terms_Code"
			,"Due_Date"
			,"Payment_Discount_Percent"
			,"Pmt_Discount_Date"
			,"Shipment_Method_Code"
			,"Location_Code"
			,"Shortcut_Dimension1_Code"
			,"Shortcut_Dimension2_Code"
			,"Vendor_Posting_Group"
			,"Currency_Code"
			,"Currency_Factor"
			,"Prices_Including_VAT"
			,"Invoice_Disc_Code"
			,"Language_Code"
			,"Purchaser_Code"
			,"Order_Class"
			,"Comment"
			,"Number_Printed"
			,"On_Hold"
			,"Applies_To_Doc_Type"
			,"Applies_To_Doc_Number"
			,"Bal_Account_Number"
			,"Recalculate_Invoice_Disc"
			,"Receive"
			,"Invoice"
			,"Print_Posted_Documents"
			,"Amount"
			,"Amount_Including_VAT"
			,"Receiving_Number"
			,"Posting_Number"
			,"Last_Receiving_Number"
			,"Last_Posting_Number"
			,"Vendor_Order_Number"
			,"Vendor_Shipment_Number"
			,"Vendor_Invoice_Number"
			,"Vendor_Cr_Memo_Number"
			,"VAT_Registration_Number"
			,"Sell_To_Customer_Number"
			,"Reason_Code"
			,"Gen_Bus_Posting_Group"
			,"Transaction_Type"
			,"Transport_Method"
			,"VAT_Country_Region_Code"
			,"Buy_From_Vendor_Name"
			,"Buy_From_Vendor_Name2"
			,"Buy_From_Address"
			,"Buy_From_Address2"
			,"Buy_From_City"
			,"Buy_From_Contact"
			,"Pay_To_Post_Code"
			,"Pay_To_County"
			,"Pay_To_Country_Region_Code"
			,"Buy_From_Post_Code"
			,"Buy_From_County"
			,"Buy_From_Country_Region_Code"
			,"Ship_To_Post_Code"
			,"Ship_To_County"
			,"Ship_To_Country_Region_Code"
			,"Bal_Account_Type"
			,"Order_Address_Code"
			,"Entry_Point"
			,"Correction"
			,"Document_Date"
			,"Area"
			,"Transaction_Specification"
			,"Payment_Method_Code"
			,"Number_Series"
			,"Posting_Number_Series"
			,"Receiving_Number_Series"
			,"Tax_Area_Code"
			,"Tax_Liable"
			,"VAT_Bus_Posting_Group"
			,"Applies_To_Id"
			,"VAT_Base_Discount_Percent"
			,"Status"
			,"Invoice_Discount_Calculation"
			,"Invoice_Discount_Value"
			,"Send_IC_Document"
			,"IC_Status"
			,"Buy_From_IC_Partner_Code"
			,"Pay_To_IC_Partner_Code"
			,"IC_Direction"
			,"Prepayment_Number"
			,"Last_Prepayment_Number"
			,"Prepmt_Cr_Memo_Number"
			,"Last_Prepmt_Cr_Memo_Number"
			,"Prepayment_Percent"
			,"Prepayment_Number_Series"
			,"Compress_Prepayment"
			,"Prepayment_Due_Date"
			,"Prepmt_Cr_Memo_Number_Series"
			,"Prepmt_Posting_Description"
			,"Prepmt_Pmt_Discount_Date"
			,"Prepmt_Payment_Terms_Code"
			,"Prepmt_Payment_Discount_Percent"
			,"Quote_Number"
			,"Job_Queue_Status"
			,"Job_Queue_Entry_Id"
			,"Incoming_Document_Entry_Number"
			,"Creditor_Number"
			,"Payment_Reference"
			,"A_Rcd_Not_Inv_Ex_Vat_LCY"
			,"Amt_Rcd_Not_Invoiced_LCY"
			,"Dimension_Set_Id"
			,"Invoice_Discount_Amount"
			,"Number_Of_Archived_Versions"
			,"Doc_Number_Occurrence"
			,"Campaign_Number"
			,"Buy_From_Contact_Number"
			,"Pay_To_Contact_Number"
			,"Responsibility_Center"
			,"Completely_Received"
			,"Posting_From_Whse_Ref"
			,"Location_Filter"
			,"Requested_Receipt_Date"
			,"Promised_Receipt_Date"
			,"Lead_Time_Calculation"
			,"Inbound_Whse_Handling_Time"
			,"Vendor_Authorization_Number"
			,"Return_Shipment_Number"
			,"Return_Shipment_Number_Series"
			,"Ship"
			,"Last_Return_Shipment_Number"
			,"Assigned_User_Id"
			,"Pending_Approvals"
			,"System_Modified_At"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pdh."documentType"
			,pdh."buyFromVendorNumber"
			,CONCAT(%L, '_', pdh."buyFromVendorNumber")
			,pdh."number"
			,CONCAT(%L, '_', pdh."number")
			,pdh."payToVendorNumber"
			,pdh."payToName"
			,pdh."payToName2"
			,pdh."payToAddress"
			,pdh."payToAddress2"
			,pdh."payToCity"
			,pdh."payToContact"
			,pdh."yourReference"
			,pdh."shipToCode"
			,pdh."shipToName"
			,pdh."shipToName2"
			,pdh."shipToAddress"
			,pdh."shipToAddress2"
			,pdh."shipToCity"
			,pdh."shipToContact"
			,pdh."orderDate"
			,pdh."postingDate"
			,pdh."expectedReceiptDate"
			,pdh."postingDescription"
			,pdh."paymentTermsCode"
			,pdh."dueDate"
			,pdh."paymentDiscountPercent"
			,pdh."pmtDiscountDate"
			,pdh."shipmentMethodCode"
			,pdh."locationCode"
			,pdh."shortcutDimension1Code"
			,pdh."shortcutDimension2Code"
			,pdh."vendorPostingGroup"
			,pdh."currencyCode"
			,pdh."currencyFactor"
			,pdh."pricesIncludingVat"
			,pdh."invoiceDiscCode"
			,pdh."languageCode"
			,pdh."purchaserCode"
			,pdh."orderClass"
			,pdh."comment"
			,pdh."numberPrinted"
			,pdh."onHold"
			,pdh."appliesToDocType"
			,pdh."appliesToDocNumber"
			,pdh."balAccountNumber"
			,pdh."recalculateInvoiceDisc"
			,pdh.receive
			,pdh.invoice
			,pdh."printPostedDocuments"
			,pdh.amount
			,pdh."amountIncludingVat"
			,pdh."receivingNumber"
			,pdh."postingNumber"
			,pdh."lastReceivingNumber"
			,pdh."lastPostingNumber"
			,pdh."vendorOrderNumber"
			,pdh."vendorShipmentNumber"
			,pdh."vendorInvoiceNumber"
			,pdh."vendorCrMemoNumber"
			,pdh."vatRegistrationNumber"
			,pdh."sellToCustomerNumber"
			,pdh."reasonCode"
			,pdh."genBusPostingGroup"
			,pdh."transactionType"
			,pdh."transportMethod"
			,pdh."vatCountryRegionCode"
			,pdh."buyFromVendorName"
			,pdh."buyFromVendorName2"
			,pdh."buyFromAddress"
			,pdh."buyFromAddress2"
			,pdh."buyFromCity"
			,pdh."buyFromContact"
			,pdh."payToPostCode"
			,pdh."payToCounty"
			,pdh."payToCountryRegionCode"
			,pdh."buyFromPostCode"
			,pdh."buyFromCounty"
			,pdh."buyFromCountryRegionCode"
			,pdh."shipToPostCode"
			,pdh."shipToCounty"
			,pdh."shipToCountryRegionCode"
			,pdh."balAccountType"
			,pdh."orderAddressCode"
			,pdh."entryPoint"
			,pdh.correction
			,pdh."documentDate"
			,pdh.area
			,pdh."transactionSpecification"
			,pdh."paymentMethodCode"
			,pdh."numberSeries"
			,pdh."postingNumberSeries"
			,pdh."receivingNumberSeries"
			,pdh."taxAreaCode"
			,pdh."taxLiable"
			,pdh."vatBusPostingGroup"
			,pdh."appliesToId"
			,pdh."vatBaseDiscountPercent"
			,pdh.status
			,pdh."invoiceDiscountCalculation"
			,pdh."invoiceDiscountValue"
			,pdh."sendIcDocument"
			,pdh."icStatus"
			,pdh."buyFromIcPartnerCode"
			,pdh."payToIcPartnerCode"
			,pdh."icDirection"
			,pdh."prepaymentNumber"
			,pdh."lastPrepaymentNumber"
			,pdh."prepmtCrMemoNumber"
			,pdh."lastPrepmtCrMemoNumber"
			,pdh."prepaymentPercent"
			,pdh."prepaymentNumberSeries"
			,pdh."compressPrepayment"
			,pdh."prepaymentDueDate"
			,pdh."prepmtCrMemoNumberSeries"
			,pdh."prepmtPostingDescription"
			,pdh."prepmtPmtDiscountDate"
			,pdh."prepmtPaymentTermsCode"
			,pdh."prepmtPaymentDiscountPercent"
			,pdh."quoteNumber"
			,pdh."jobQueueStatus"
			,pdh."jobQueueEntryId"
			,pdh."incomingDocumentEntryNumber"
			,pdh."creditorNumber"
			,pdh."paymentReference"
			,pdh."aRcdNotInvExVatLcy"
			,pdh."amtRcdNotInvoicedLcy"
			,pdh."dimensionSetId"
			,pdh."invoiceDiscountAmount"
			,pdh."numberOfArchivedVersions"
			,pdh."docNumberOccurrence"
			,pdh."campaignNumber"
			,pdh."buyFromContactNumber"
			,pdh."payToContactNumber"
			,pdh."responsibilityCenter"
			,pdh."completelyReceived"
			,pdh."postingFromWhseRef"
			,pdh."locationFilter"
			,pdh."requestedReceiptDate"
			,pdh."promisedReceiptDate"
			,pdh."leadTimeCalculation"
			,pdh."inboundWhseHandlingTime"
			,pdh."vendorAuthorizationNumber"
			,pdh."returnShipmentNumber"
			,pdh."returnShipmentNumberSeries"
			,pdh.ship
			,pdh."lastReturnShipmentNumber"
			,pdh."assignedUserId"
			,pdh."pendingApprovals"
			,pdh."SystemModifiedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pdh

    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_purchase_documents_header()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_purchase_documents_header_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Document_Type"
		,"Buy_From_Vendor_Number"
		,"Key_Buy_From_Vendor_Number"
		,"Number"
		,"Key_Number"
		,"Pay_To_Vendor_Number"
		,"Pay_To_Name"
		,"Pay_To_Name2"
		,"Pay_To_Address"
		,"Pay_To_Address2"
		,"Pay_To_City"
		,"Pay_To_Contact"
		,"Your_Reference"
		,"Ship_To_Code"
		,"Ship_To_Name"
		,"Ship_To_Name2"
		,"Ship_To_Address"
		,"Ship_To_Address2"
		,"Ship_To_City"
		,"Ship_To_Contact"
		,"Order_Date"
		,"Posting_Date"
		,"Expected_Receipt_Date"
		,"Posting_Description"
		,"Payment_Terms_Code"
		,"Due_Date"
		,"Payment_Discount_Percent"
		,"Pmt_Discount_Date"
		,"Shipment_Method_Code"
		,"Location_Code"
		,"Shortcut_Dimension1_Code"
		,"Shortcut_Dimension2_Code"
		,"Vendor_Posting_Group"
		,"Currency_Code"
		,"Currency_Factor"
		,"Prices_Including_VAT"
		,"Invoice_Disc_Code"
		,"Language_Code"
		,"Purchaser_Code"
		,"Order_Class"
		,"Comment"
		,"Number_Printed"
		,"On_Hold"
		,"Applies_To_Doc_Type"
		,"Applies_To_Doc_Number"
		,"Bal_Account_Number"
		,"Recalculate_Invoice_Disc"
		,"Receive"
		,"Invoice"
		,"Print_Posted_Documents"
		,"Amount"
		,"Amount_Including_VAT"
		,"Receiving_Number"
		,"Posting_Number"
		,"Last_Receiving_Number"
		,"Last_Posting_Number"
		,"Vendor_Order_Number"
		,"Vendor_Shipment_Number"
		,"Vendor_Invoice_Number"
		,"Vendor_Cr_Memo_Number"
		,"VAT_Registration_Number"
		,"Sell_To_Customer_Number"
		,"Reason_Code"
		,"Gen_Bus_Posting_Group"
		,"Transaction_Type"
		,"Transport_Method"
		,"VAT_Country_Region_Code"
		,"Buy_From_Vendor_Name"
		,"Buy_From_Vendor_Name2"
		,"Buy_From_Address"
		,"Buy_From_Address2"
		,"Buy_From_City"
		,"Buy_From_Contact"
		,"Pay_To_Post_Code"
		,"Pay_To_County"
		,"Pay_To_Country_Region_Code"
		,"Buy_From_Post_Code"
		,"Buy_From_County"
		,"Buy_From_Country_Region_Code"
		,"Ship_To_Post_Code"
		,"Ship_To_County"
		,"Ship_To_Country_Region_Code"
		,"Bal_Account_Type"
		,"Order_Address_Code"
		,"Entry_Point"
		,"Correction"
		,"Document_Date"
		,"Area"
		,"Transaction_Specification"
		,"Payment_Method_Code"
		,"Number_Series"
		,"Posting_Number_Series"
		,"Receiving_Number_Series"
		,"Tax_Area_Code"
		,"Tax_Liable"
		,"VAT_Bus_Posting_Group"
		,"Applies_To_Id"
		,"VAT_Base_Discount_Percent"
		,"Status"
		,"Invoice_Discount_Calculation"
		,"Invoice_Discount_Value"
		,"Send_IC_Document"
		,"IC_Status"
		,"Buy_From_IC_Partner_Code"
		,"Pay_To_IC_Partner_Code"
		,"IC_Direction"
		,"Prepayment_Number"
		,"Last_Prepayment_Number"
		,"Prepmt_Cr_Memo_Number"
		,"Last_Prepmt_Cr_Memo_Number"
		,"Prepayment_Percent"
		,"Prepayment_Number_Series"
		,"Compress_Prepayment"
		,"Prepayment_Due_Date"
		,"Prepmt_Cr_Memo_Number_Series"
		,"Prepmt_Posting_Description"
		,"Prepmt_Pmt_Discount_Date"
		,"Prepmt_Payment_Terms_Code"
		,"Prepmt_Payment_Discount_Percent"
		,"Quote_Number"
		,"Job_Queue_Status"
		,"Job_Queue_Entry_Id"
		,"Incoming_Document_Entry_Number"
		,"Creditor_Number"
		,"Payment_Reference"
		,"A_Rcd_Not_Inv_Ex_Vat_LCY"
		,"Amt_Rcd_Not_Invoiced_LCY"
		,"Dimension_Set_Id"
		,"Invoice_Discount_Amount"
		,"Number_Of_Archived_Versions"
		,"Doc_Number_Occurrence"
		,"Campaign_Number"
		,"Buy_From_Contact_Number"
		,"Pay_To_Contact_Number"
		,"Responsibility_Center"
		,"Completely_Received"
		,"Posting_From_Whse_Ref"
		,"Location_Filter"
		,"Requested_Receipt_Date"
		,"Promised_Receipt_Date"
		,"Lead_Time_Calculation"
		,"Inbound_Whse_Handling_Time"
		,"Vendor_Authorization_Number"
		,"Return_Shipment_Number"
		,"Return_Shipment_Number_Series"
		,"Ship"
		,"Last_Return_Shipment_Number"
		,"Assigned_User_Id"
		,"Pending_Approvals"
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
		$151, $152	  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Document_Type", "Number") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"Buy_From_Vendor_Number" = EXCLUDED."Buy_From_Vendor_Number"
		,"Key_Buy_From_Vendor_Number" = EXCLUDED."Key_Buy_From_Vendor_Number"
		,"Number" = EXCLUDED."Number"
		,"Key_Number" = EXCLUDED."Key_Number"
		,"Pay_To_Vendor_Number" = EXCLUDED."Pay_To_Vendor_Number"
		,"Pay_To_Name" = EXCLUDED."Pay_To_Name"
		,"Pay_To_Name2" = EXCLUDED."Pay_To_Name2"
		,"Pay_To_Address" = EXCLUDED."Pay_To_Address"
		,"Pay_To_Address2" = EXCLUDED."Pay_To_Address2"
		,"Pay_To_City" = EXCLUDED."Pay_To_City"
		,"Pay_To_Contact" = EXCLUDED."Pay_To_Contact"
		,"Your_Reference" = EXCLUDED."Your_Reference"
		,"Ship_To_Code" = EXCLUDED."Ship_To_Code"
		,"Ship_To_Name" = EXCLUDED."Ship_To_Name"
		,"Ship_To_Name2" = EXCLUDED."Ship_To_Name2"
		,"Ship_To_Address" = EXCLUDED."Ship_To_Address"
		,"Ship_To_Address2" = EXCLUDED."Ship_To_Address2"
		,"Ship_To_City" = EXCLUDED."Ship_To_City"
		,"Ship_To_Contact" = EXCLUDED."Ship_To_Contact"
		,"Order_Date" = EXCLUDED."Order_Date"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Expected_Receipt_Date" = EXCLUDED."Expected_Receipt_Date"
		,"Posting_Description" = EXCLUDED."Posting_Description"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Payment_Discount_Percent" = EXCLUDED."Payment_Discount_Percent"
		,"Pmt_Discount_Date" = EXCLUDED."Pmt_Discount_Date"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Shortcut_Dimension1_Code" = EXCLUDED."Shortcut_Dimension1_Code"
		,"Shortcut_Dimension2_Code" = EXCLUDED."Shortcut_Dimension2_Code"
		,"Vendor_Posting_Group" = EXCLUDED."Vendor_Posting_Group"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Currency_Factor" = EXCLUDED."Currency_Factor"
		,"Prices_Including_VAT" = EXCLUDED."Prices_Including_VAT"
		,"Invoice_Disc_Code" = EXCLUDED."Invoice_Disc_Code"
		,"Language_Code" = EXCLUDED."Language_Code"
		,"Purchaser_Code" = EXCLUDED."Purchaser_Code"
		,"Order_Class" = EXCLUDED."Order_Class"
		,"Comment" = EXCLUDED."Comment"
		,"Number_Printed" = EXCLUDED."Number_Printed"
		,"On_Hold" = EXCLUDED."On_Hold"
		,"Applies_To_Doc_Type" = EXCLUDED."Applies_To_Doc_Type"
		,"Applies_To_Doc_Number" = EXCLUDED."Applies_To_Doc_Number"
		,"Bal_Account_Number" = EXCLUDED."Bal_Account_Number"
		,"Recalculate_Invoice_Disc" = EXCLUDED."Recalculate_Invoice_Disc"
		,"Receive" = EXCLUDED."Receive"
		,"Invoice" = EXCLUDED."Invoice"
		,"Print_Posted_Documents" = EXCLUDED."Print_Posted_Documents"
		,"Amount" = EXCLUDED."Amount"
		,"Amount_Including_VAT" = EXCLUDED."Amount_Including_VAT"
		,"Receiving_Number" = EXCLUDED."Receiving_Number"
		,"Posting_Number" = EXCLUDED."Posting_Number"
		,"Last_Receiving_Number" = EXCLUDED."Last_Receiving_Number"
		,"Last_Posting_Number" = EXCLUDED."Last_Posting_Number"
		,"Vendor_Order_Number" = EXCLUDED."Vendor_Order_Number"
		,"Vendor_Shipment_Number" = EXCLUDED."Vendor_Shipment_Number"
		,"Vendor_Invoice_Number" = EXCLUDED."Vendor_Invoice_Number"
		,"Vendor_Cr_Memo_Number" = EXCLUDED."Vendor_Cr_Memo_Number"
		,"VAT_Registration_Number" = EXCLUDED."VAT_Registration_Number"
		,"Sell_To_Customer_Number" = EXCLUDED."Sell_To_Customer_Number"
		,"Reason_Code" = EXCLUDED."Reason_Code"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Transaction_Type" = EXCLUDED."Transaction_Type"
		,"Transport_Method" = EXCLUDED."Transport_Method"
		,"VAT_Country_Region_Code" = EXCLUDED."VAT_Country_Region_Code"
		,"Buy_From_Vendor_Name" = EXCLUDED."Buy_From_Vendor_Name"
		,"Buy_From_Vendor_Name2" = EXCLUDED."Buy_From_Vendor_Name2"
		,"Buy_From_Address" = EXCLUDED."Buy_From_Address"
		,"Buy_From_Address2" = EXCLUDED."Buy_From_Address2"
		,"Buy_From_City" = EXCLUDED."Buy_From_City"
		,"Buy_From_Contact" = EXCLUDED."Buy_From_Contact"
		,"Pay_To_Post_Code" = EXCLUDED."Pay_To_Post_Code"
		,"Pay_To_County" = EXCLUDED."Pay_To_County"
		,"Pay_To_Country_Region_Code" = EXCLUDED."Pay_To_Country_Region_Code"
		,"Buy_From_Post_Code" = EXCLUDED."Buy_From_Post_Code"
		,"Buy_From_County" = EXCLUDED."Buy_From_County"
		,"Buy_From_Country_Region_Code" = EXCLUDED."Buy_From_Country_Region_Code"
		,"Ship_To_Post_Code" = EXCLUDED."Ship_To_Post_Code"
		,"Ship_To_County" = EXCLUDED."Ship_To_County"
		,"Ship_To_Country_Region_Code" = EXCLUDED."Ship_To_Country_Region_Code"
		,"Bal_Account_Type" = EXCLUDED."Bal_Account_Type"
		,"Order_Address_Code" = EXCLUDED."Order_Address_Code"
		,"Entry_Point" = EXCLUDED."Entry_Point"
		,"Correction" = EXCLUDED."Correction"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Area" = EXCLUDED."Area"
		,"Transaction_Specification" = EXCLUDED."Transaction_Specification"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Number_Series" = EXCLUDED."Number_Series"
		,"Posting_Number_Series" = EXCLUDED."Posting_Number_Series"
		,"Receiving_Number_Series" = EXCLUDED."Receiving_Number_Series"
		,"Tax_Area_Code" = EXCLUDED."Tax_Area_Code"
		,"Tax_Liable" = EXCLUDED."Tax_Liable"
		,"VAT_Bus_Posting_Group" = EXCLUDED."VAT_Bus_Posting_Group"
		,"Applies_To_Id" = EXCLUDED."Applies_To_Id"
		,"VAT_Base_Discount_Percent" = EXCLUDED."VAT_Base_Discount_Percent"
		,"Status" = EXCLUDED."Status"
		,"Invoice_Discount_Calculation" = EXCLUDED."Invoice_Discount_Calculation"
		,"Invoice_Discount_Value" = EXCLUDED."Invoice_Discount_Value"
		,"Send_IC_Document" = EXCLUDED."Send_IC_Document"
		,"IC_Status" = EXCLUDED."IC_Status"
		,"Buy_From_IC_Partner_Code" = EXCLUDED."Buy_From_IC_Partner_Code"
		,"Pay_To_IC_Partner_Code" = EXCLUDED."Pay_To_IC_Partner_Code"
		,"IC_Direction" = EXCLUDED."IC_Direction"
		,"Prepayment_Number" = EXCLUDED."Prepayment_Number"
		,"Last_Prepayment_Number" = EXCLUDED."Last_Prepayment_Number"
		,"Prepmt_Cr_Memo_Number" = EXCLUDED."Prepmt_Cr_Memo_Number"
		,"Last_Prepmt_Cr_Memo_Number" = EXCLUDED."Last_Prepmt_Cr_Memo_Number"
		,"Prepayment_Percent" = EXCLUDED."Prepayment_Percent"
		,"Prepayment_Number_Series" = EXCLUDED."Prepayment_Number_Series"
		,"Compress_Prepayment" = EXCLUDED."Compress_Prepayment"
		,"Prepayment_Due_Date" = EXCLUDED."Prepayment_Due_Date"
		,"Prepmt_Cr_Memo_Number_Series" = EXCLUDED."Prepmt_Cr_Memo_Number_Series"
		,"Prepmt_Posting_Description" = EXCLUDED."Prepmt_Posting_Description"
		,"Prepmt_Pmt_Discount_Date" = EXCLUDED."Prepmt_Pmt_Discount_Date"
		,"Prepmt_Payment_Terms_Code" = EXCLUDED."Prepmt_Payment_Terms_Code"
		,"Prepmt_Payment_Discount_Percent" = EXCLUDED."Prepmt_Payment_Discount_Percent"
		,"Quote_Number" = EXCLUDED."Quote_Number"
		,"Job_Queue_Status" = EXCLUDED."Job_Queue_Status"
		,"Job_Queue_Entry_Id" = EXCLUDED."Job_Queue_Entry_Id"
		,"Incoming_Document_Entry_Number" = EXCLUDED."Incoming_Document_Entry_Number"
		,"Creditor_Number" = EXCLUDED."Creditor_Number"
		,"Payment_Reference" = EXCLUDED."Payment_Reference"
		,"A_Rcd_Not_Inv_Ex_Vat_LCY" = EXCLUDED."A_Rcd_Not_Inv_Ex_Vat_LCY"
		,"Amt_Rcd_Not_Invoiced_LCY" = EXCLUDED."Amt_Rcd_Not_Invoiced_LCY"
		,"Dimension_Set_Id" = EXCLUDED."Dimension_Set_Id"
		,"Invoice_Discount_Amount" = EXCLUDED."Invoice_Discount_Amount"
		,"Number_Of_Archived_Versions" = EXCLUDED."Number_Of_Archived_Versions"
		,"Doc_Number_Occurrence" = EXCLUDED."Doc_Number_Occurrence"
		,"Campaign_Number" = EXCLUDED."Campaign_Number"
		,"Buy_From_Contact_Number" = EXCLUDED."Buy_From_Contact_Number"
		,"Pay_To_Contact_Number" = EXCLUDED."Pay_To_Contact_Number"
		,"Responsibility_Center" = EXCLUDED."Responsibility_Center"
		,"Completely_Received" = EXCLUDED."Completely_Received"
		,"Posting_From_Whse_Ref" = EXCLUDED."Posting_From_Whse_Ref"
		,"Location_Filter" = EXCLUDED."Location_Filter"
		,"Requested_Receipt_Date" = EXCLUDED."Requested_Receipt_Date"
		,"Promised_Receipt_Date" = EXCLUDED."Promised_Receipt_Date"
		,"Lead_Time_Calculation" = EXCLUDED."Lead_Time_Calculation"
		,"Inbound_Whse_Handling_Time" = EXCLUDED."Inbound_Whse_Handling_Time"
		,"Vendor_Authorization_Number" = EXCLUDED."Vendor_Authorization_Number"
		,"Return_Shipment_Number" = EXCLUDED."Return_Shipment_Number"
		,"Return_Shipment_Number_Series" = EXCLUDED."Return_Shipment_Number_Series"
		,"Ship" = EXCLUDED."Ship"
		,"Last_Return_Shipment_Number" = EXCLUDED."Last_Return_Shipment_Number"
		,"Assigned_User_Id" = EXCLUDED."Assigned_User_Id"
		,"Pending_Approvals" = EXCLUDED."Pending_Approvals"
		,"System_Modified_At" = EXCLUDED."System_Modified_At"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."documentType"
		,NEW."buyFromVendorNumber"
		,CONCAT(litera_firmy, '_', NEW."buyFromVendorNumber")
		,NEW."number"
		,CONCAT(litera_firmy, '_', NEW."number")
		,NEW."payToVendorNumber"
		,NEW."payToName"
		,NEW."payToName2"
		,NEW."payToAddress"
		,NEW."payToAddress2"
		,NEW."payToCity"
		,NEW."payToContact"
		,NEW."yourReference"
		,NEW."shipToCode"
		,NEW."shipToName"
		,NEW."shipToName2"
		,NEW."shipToAddress"
		,NEW."shipToAddress2"
		,NEW."shipToCity"
		,NEW."shipToContact"
		,NEW."orderDate"
		,NEW."postingDate"
		,NEW."expectedReceiptDate"
		,NEW."postingDescription"
		,NEW."paymentTermsCode"
		,NEW."dueDate"
		,NEW."paymentDiscountPercent"
		,NEW."pmtDiscountDate"
		,NEW."shipmentMethodCode"
		,NEW."locationCode"
		,NEW."shortcutDimension1Code"
		,NEW."shortcutDimension2Code"
		,NEW."vendorPostingGroup"
		,NEW."currencyCode"
		,NEW."currencyFactor"
		,NEW."pricesIncludingVat"
		,NEW."invoiceDiscCode"
		,NEW."languageCode"
		,NEW."purchaserCode"
		,NEW."orderClass"
		,NEW."comment"
		,NEW."numberPrinted"
		,NEW."onHold"
		,NEW."appliesToDocType"
		,NEW."appliesToDocNumber"
		,NEW."balAccountNumber"
		,NEW."recalculateInvoiceDisc"
		,NEW.receive
		,NEW.invoice
		,NEW."printPostedDocuments"
		,NEW.amount
		,NEW."amountIncludingVat"
		,NEW."receivingNumber"
		,NEW."postingNumber"
		,NEW."lastReceivingNumber"
		,NEW."lastPostingNumber"
		,NEW."vendorOrderNumber"
		,NEW."vendorShipmentNumber"
		,NEW."vendorInvoiceNumber"
		,NEW."vendorCrMemoNumber"
		,NEW."vatRegistrationNumber"
		,NEW."sellToCustomerNumber"
		,NEW."reasonCode"
		,NEW."genBusPostingGroup"
		,NEW."transactionType"
		,NEW."transportMethod"
		,NEW."vatCountryRegionCode"
		,NEW."buyFromVendorName"
		,NEW."buyFromVendorName2"
		,NEW."buyFromAddress"
		,NEW."buyFromAddress2"
		,NEW."buyFromCity"
		,NEW."buyFromContact"
		,NEW."payToPostCode"
		,NEW."payToCounty"
		,NEW."payToCountryRegionCode"
		,NEW."buyFromPostCode"
		,NEW."buyFromCounty"
		,NEW."buyFromCountryRegionCode"
		,NEW."shipToPostCode"
		,NEW."shipToCounty"
		,NEW."shipToCountryRegionCode"
		,NEW."balAccountType"
		,NEW."orderAddressCode"
		,NEW."entryPoint"
		,NEW.correction
		,NEW."documentDate"
		,NEW.area
		,NEW."transactionSpecification"
		,NEW."paymentMethodCode"
		,NEW."numberSeries"
		,NEW."postingNumberSeries"
		,NEW."receivingNumberSeries"
		,NEW."taxAreaCode"
		,NEW."taxLiable"
		,NEW."vatBusPostingGroup"
		,NEW."appliesToId"
		,NEW."vatBaseDiscountPercent"
		,NEW.status
		,NEW."invoiceDiscountCalculation"
		,NEW."invoiceDiscountValue"
		,NEW."sendIcDocument"
		,NEW."icStatus"
		,NEW."buyFromIcPartnerCode"
		,NEW."payToIcPartnerCode"
		,NEW."icDirection"
		,NEW."prepaymentNumber"
		,NEW."lastPrepaymentNumber"
		,NEW."prepmtCrMemoNumber"
		,NEW."lastPrepmtCrMemoNumber"
		,NEW."prepaymentPercent"
		,NEW."prepaymentNumberSeries"
		,NEW."compressPrepayment"
		,NEW."prepaymentDueDate"
		,NEW."prepmtCrMemoNumberSeries"
		,NEW."prepmtPostingDescription"
		,NEW."prepmtPmtDiscountDate"
		,NEW."prepmtPaymentTermsCode"
		,NEW."prepmtPaymentDiscountPercent"
		,NEW."quoteNumber"
		,NEW."jobQueueStatus"
		,NEW."jobQueueEntryId"
		,NEW."incomingDocumentEntryNumber"
		,NEW."creditorNumber"
		,NEW."paymentReference"
		,NEW."aRcdNotInvExVatLcy"
		,NEW."amtRcdNotInvoicedLcy"
		,NEW."dimensionSetId"
		,NEW."invoiceDiscountAmount"
		,NEW."numberOfArchivedVersions"
		,NEW."docNumberOccurrence"
		,NEW."campaignNumber"
		,NEW."buyFromContactNumber"
		,NEW."payToContactNumber"
		,NEW."responsibilityCenter"
		,NEW."completelyReceived"
		,NEW."postingFromWhseRef"
		,NEW."locationFilter"
		,NEW."requestedReceiptDate"
		,NEW."promisedReceiptDate"
		,NEW."leadTimeCalculation"
		,NEW."inboundWhseHandlingTime"
		,NEW."vendorAuthorizationNumber"
		,NEW."returnShipmentNumber"
		,NEW."returnShipmentNumberSeries"
		,NEW.ship
		,NEW."lastReturnShipmentNumber"
		,NEW."assignedUserId"
		,NEW."pendingApprovals"
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
	grupa_tabel text := 'purchase_documents_header'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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