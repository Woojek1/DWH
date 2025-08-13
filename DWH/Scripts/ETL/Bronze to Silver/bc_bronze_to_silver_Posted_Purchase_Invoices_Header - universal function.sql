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
	_tabela := format('bc_posted_purchase_invoice_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text NOT NULL
			,"Key_No" text PRIMARY KEY
			,"Buy_from_Vendor_No" text NULL
			,"Buy_from_Vendor_Name" text NULL
			,"Buy_from_Address" text NULL
			,"Buy_from_Address_2" text NULL
			,"Buy_from_City" text NULL
			,"Buy_from_County" text NULL
			,"Buy_from_Post_Code" text NULL
			,"Buy_from_Country_Region_Code" text NULL
			,"Buy_from_Contact_No" text NULL
			,"BuyFromContactPhoneNo" text NULL
			,"BuyFromContactMobilePhoneNo" text NULL
			,"BuyFromContactEmail" text NULL
			,"Buy_from_Contact" text NULL
			,"ITI_Posting_Desc_Code" text NULL
			,"ITI_Posting_Description" text NULL
			,"Posting_Date" date NULL
			,"VAT_Reporting_Date" date NULL
			,"Document_Date" date NULL
			,"ITI_Document_Receipt_Date" date NULL
			,"ITI_Rev_Chrg_Purchase_Delivery_Date" date NULL
			,"ITI_VAT_Settlement_Date" date NULL
			,"ITI_Postponed_VAT" bool NULL
			,"ITI_Disable_NSeI_for_Purchase" bool NULL
			,"Due_Date" date NULL
			,"Quote_No" text NULL
			,"Order_No" text NULL
			,"ITI_SAFT_Ext_Document_No" text NULL
			,"ITI_NSeI_Document_No" text NULL
			,"Vendor_Invoice_No" text NULL
			,"Vendor_Order_No" text NULL
			,"Pre_Assigned_No" text NULL
			,"No_Printed" int4 NULL
			,"Order_Address_Code" text NULL
			,"Purchaser_Code" text NULL
			,"Responsibility_Center" text NULL
			,"Cancelled" bool NULL
			,"Corrective" bool NULL
			,"EDN_OGL_Declaration" text NULL
			,"EDN_Equivalent_Set" text NULL
			,"EDN_Purchase_Information" text NULL
			,"EDN_Container_No" text NULL
			,"EDN_Container_Type_Code" text NULL
			,"Currency_Code" text NULL
			,"EDN_Currency_Date" date NULL
			,"EDN_Curr_Exchange_Rate" numeric(14, 2) NULL
			,"ITI_Split_Payment" bool NULL
			,"EU_3_Party_Trade" bool NULL
			,"Expected_Receipt_Date" date NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Payment_Discount_Percent" numeric(6, 2) NULL
			,"Pmt_Discount_Date" date NULL
			,"Tax_Liable" bool NULL
			,"Tax_Area_Code" text NULL
			,"Location_Code" text NULL
			,"Shipment_Method_Code" text NULL
			,"Payment_Reference" text NULL
			,"Creditor_No" text NULL
			,"ITI_VATRegNoOrigCountryCode" text NULL
			,"ITI_VAT_Registration_No" text NULL
			,"ITI_VAT_Status_Code" text NULL
			,"Vendor_Posting_Group" text NULL
			,"ITI_Recipient_Bank_Account" text NULL
			,"Ship_to_Code" text NULL
			,"Ship_to_Name" text NULL
			,"Ship_to_Address" text NULL
			,"Ship_to_Address_2" text NULL
			,"Ship_to_City" text NULL
			,"Ship_to_County" text NULL
			,"Ship_to_Post_Code" text NULL
			,"Ship_to_Country_Region_Code" text NULL
			,"Ship_to_Contact" text NULL
			,"Applicable_For_Serv_Decl" bool NULL
			,"Pay_to_Name" text NULL
			,"Pay_to_Address" text NULL
			,"Pay_to_Address_2" text NULL
			,"Pay_to_City" text NULL
			,"Pay_to_County" text NULL
			,"Pay_to_Post_Code" text NULL
			,"Pay_to_Country_Region_Code" text NULL
			,"Pay_to_Contact_No" text NULL
			,"PayToContactPhoneNo" text NULL
			,"PayToContactMobilePhoneNo" text NULL
			,"PayToContactEmail" text NULL
			,"Pay_to_Contact" text NULL
			,"Remit_to_Code" text NULL
			,"Remit_to_Name" text NULL
			,"Remit_to_Address" text NULL
			,"Remit_to_Address_2" text NULL
			,"Remit_to_City" text NULL
			,"Remit_to_County" text NULL
			,"Remit_to_Post_Code" text NULL
			,"Remit_to_Country_Region_Code" text NULL
			,"Remit_to_Contact" text NULL
			,"SystemModifiedAt" timestamptz NULL
			,"Invoice_Type" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Key_No"
			,"Buy_from_Vendor_No"
			,"Buy_from_Vendor_Name"
			,"Buy_from_Address"
			,"Buy_from_Address_2"
			,"Buy_from_City"
			,"Buy_from_County"
			,"Buy_from_Post_Code"
			,"Buy_from_Country_Region_Code"
			,"Buy_from_Contact_No"
			,"BuyFromContactPhoneNo"
			,"BuyFromContactMobilePhoneNo"
			,"BuyFromContactEmail"
			,"Buy_from_Contact"
			,"ITI_Posting_Desc_Code"
			,"ITI_Posting_Description"
			,"Posting_Date"
			,"VAT_Reporting_Date"
			,"Document_Date"
			,"ITI_Document_Receipt_Date"
			,"ITI_Rev_Chrg_Purchase_Delivery_Date"
			,"ITI_VAT_Settlement_Date"
			,"ITI_Postponed_VAT"
			,"ITI_Disable_NSeI_for_Purchase"
			,"Due_Date"
			,"Quote_No"
			,"Order_No"
			,"ITI_SAFT_Ext_Document_No"
			,"ITI_NSeI_Document_No"
			,"Vendor_Invoice_No"
			,"Vendor_Order_No"
			,"Pre_Assigned_No"
			,"No_Printed"
			,"Order_Address_Code"
			,"Purchaser_Code"
			,"Responsibility_Center"
			,"Cancelled"
			,"Corrective"
			,"EDN_OGL_Declaration"
			,"EDN_Equivalent_Set"
			,"EDN_Purchase_Information"
			,"EDN_Container_No"
			,"EDN_Container_Type_Code"
			,"Currency_Code"
			,"EDN_Currency_Date"
			,"EDN_Curr_Exchange_Rate"
			,"ITI_Split_Payment"
			,"EU_3_Party_Trade"
			,"Expected_Receipt_Date"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Payment_Discount_Percent"
			,"Pmt_Discount_Date"
			,"Tax_Liable"
			,"Tax_Area_Code"
			,"Location_Code"
			,"Shipment_Method_Code"
			,"Payment_Reference"
			,"Creditor_No"
			,"ITI_VATRegNoOrigCountryCode"
			,"ITI_VAT_Registration_No"
			,"ITI_VAT_Status_Code"
			,"Vendor_Posting_Group"
			,"ITI_Recipient_Bank_Account"
			,"Ship_to_Code"
			,"Ship_to_Name"
			,"Ship_to_Address"
			,"Ship_to_Address_2"
			,"Ship_to_City"
			,"Ship_to_County"
			,"Ship_to_Post_Code"
			,"Ship_to_Country_Region_Code"
			,"Ship_to_Contact"
			,"Applicable_For_Serv_Decl"
			,"Pay_to_Name"
			,"Pay_to_Address"
			,"Pay_to_Address_2"
			,"Pay_to_City"
			,"Pay_to_County"
			,"Pay_to_Post_Code"
			,"Pay_to_Country_Region_Code"
			,"Pay_to_Contact_No"
			,"PayToContactPhoneNo"
			,"PayToContactMobilePhoneNo"
			,"PayToContactEmail"
			,"Pay_to_Contact"
			,"Remit_to_Code"
			,"Remit_to_Name"
			,"Remit_to_Address"
			,"Remit_to_Address_2"
			,"Remit_to_City"
			,"Remit_to_County"
			,"Remit_to_Post_Code"
			,"Remit_to_Country_Region_Code"
			,"Remit_to_Contact"
			,"SystemModifiedAt"
			,"Invoice_Type"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pih."No"
			,concat(%L ,'_', pih."No")
			,pih."Buy_from_Vendor_No"
			,pih."Buy_from_Vendor_Name"
			,pih."Buy_from_Address"
			,pih."Buy_from_Address_2"
			,pih."Buy_from_City"
			,pih."Buy_from_County"
			,pih."Buy_from_Post_Code"
			,pih."Buy_from_Country_Region_Code"
			,pih."Buy_from_Contact_No"
			,pih."BuyFromContactPhoneNo"
			,pih."BuyFromContactMobilePhoneNo"
			,pih."BuyFromContactEmail"
			,pih."Buy_from_Contact"
			,pih."ITI_Posting_Desc_Code"
			,pih."ITI_Posting_Description"
			,pih."Posting_Date"
			,pih."VAT_Reporting_Date"
			,pih."Document_Date"
			,pih."ITI_Document_Receipt_Date"
			,pih."ITI_Rev_Chrg_Purchase_Delivery_Date"
			,pih."ITI_VAT_Settlement_Date"
			,pih."ITI_Postponed_VAT"
			,pih."ITI_Disable_NSeI_for_Purchase"
			,pih."Due_Date"
			,pih."Quote_No"
			,pih."Order_No"
			,pih."ITI_SAFT_Ext_Document_No"
			,pih."ITI_NSeI_Document_No"
			,pih."Vendor_Invoice_No"
			,pih."Vendor_Order_No"
			,pih."Pre_Assigned_No"
			,pih."No_Printed"
			,pih."Order_Address_Code"
			,pih."Purchaser_Code"
			,pih."Responsibility_Center"
			,pih."Cancelled"
			,pih."Corrective"
			,pih."EDN_OGL_Declaration"
			,pih."EDN_Equivalent_Set"
			,pih."EDN_Purchase_Information"
			,pih."EDN_Container_No"
			,pih."EDN_Container_Type_Code"
			,pih."Currency_Code"
			,pih."EDN_Currency_Date"
			,pih."EDN_Curr_Exchange_Rate"
			,pih."ITI_Split_Payment"
			,pih."EU_3_Party_Trade"
			,pih."Expected_Receipt_Date"
			,pih."Payment_Terms_Code"
			,pih."Payment_Method_Code"
			,pih."Shortcut_Dimension_1_Code"
			,pih."Shortcut_Dimension_2_Code"
			,pih."Payment_Discount_Percent"
			,pih."Pmt_Discount_Date"
			,pih."Tax_Liable"
			,pih."Tax_Area_Code"
			,pih."Location_Code"
			,pih."Shipment_Method_Code"
			,pih."Payment_Reference"
			,pih."Creditor_No"
			,pih."ITI_VATRegNoOrigCountryCode"
			,pih."ITI_VAT_Registration_No"
			,pih."ITI_VAT_Status_Code"
			,pih."Vendor_Posting_Group"
			,pih."ITI_Recipient_Bank_Account"
			,pih."Ship_to_Code"
			,pih."Ship_to_Name"
			,pih."Ship_to_Address"
			,pih."Ship_to_Address_2"
			,pih."Ship_to_City"
			,pih."Ship_to_County"
			,pih."Ship_to_Post_Code"
			,pih."Ship_to_Country_Region_Code"
			,pih."Ship_to_Contact"
			,pih."Applicable_For_Serv_Decl"
			,pih."Pay_to_Name"
			,pih."Pay_to_Address"
			,pih."Pay_to_Address_2"
			,pih."Pay_to_City"
			,pih."Pay_to_County"
			,pih."Pay_to_Post_Code"
			,pih."Pay_to_Country_Region_Code"
			,pih."Pay_to_Contact_No"
			,pih."PayToContactPhoneNo"
			,pih."PayToContactMobilePhoneNo"
			,pih."PayToContactEmail"
			,pih."Pay_to_Contact"
			,pih."Remit_to_Code"
			,pih."Remit_to_Name"
			,pih."Remit_to_Address"
			,pih."Remit_to_Address_2"
			,pih."Remit_to_City"
			,pih."Remit_to_County"
			,pih."Remit_to_Post_Code"
			,pih."Remit_to_Country_Region_Code"
			,pih."Remit_to_Contact"
			,pih."SystemModifiedAt"
			,'Faktura'
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pih


    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_purchase_invoice_header()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_purchase_invoice_header_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"Key_No"
		,"Buy_from_Vendor_No"
		,"Buy_from_Vendor_Name"
		,"Buy_from_Address"
		,"Buy_from_Address_2"
		,"Buy_from_City"
		,"Buy_from_County"
		,"Buy_from_Post_Code"
		,"Buy_from_Country_Region_Code"
		,"Buy_from_Contact_No"
		,"BuyFromContactPhoneNo"
		,"BuyFromContactMobilePhoneNo"
		,"BuyFromContactEmail"
		,"Buy_from_Contact"
		,"ITI_Posting_Desc_Code"
		,"ITI_Posting_Description"
		,"Posting_Date"
		,"VAT_Reporting_Date"
		,"Document_Date"
		,"ITI_Document_Receipt_Date"
		,"ITI_Rev_Chrg_Purchase_Delivery_Date"
		,"ITI_VAT_Settlement_Date"
		,"ITI_Postponed_VAT"
		,"ITI_Disable_NSeI_for_Purchase"
		,"Due_Date"
		,"Quote_No"
		,"Order_No"
		,"ITI_SAFT_Ext_Document_No"
		,"ITI_NSeI_Document_No"
		,"Vendor_Invoice_No"
		,"Vendor_Order_No"
		,"Pre_Assigned_No"
		,"No_Printed"
		,"Order_Address_Code"
		,"Purchaser_Code"
		,"Responsibility_Center"
		,"Cancelled"
		,"Corrective"
		,"EDN_OGL_Declaration"
		,"EDN_Equivalent_Set"
		,"EDN_Purchase_Information"
		,"EDN_Container_No"
		,"EDN_Container_Type_Code"
		,"Currency_Code"
		,"EDN_Currency_Date"
		,"EDN_Curr_Exchange_Rate"
		,"ITI_Split_Payment"
		,"EU_3_Party_Trade"
		,"Expected_Receipt_Date"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Payment_Discount_Percent"
		,"Pmt_Discount_Date"
		,"Tax_Liable"
		,"Tax_Area_Code"
		,"Location_Code"
		,"Shipment_Method_Code"
		,"Payment_Reference"
		,"Creditor_No"
		,"ITI_VATRegNoOrigCountryCode"
		,"ITI_VAT_Registration_No"
		,"ITI_VAT_Status_Code"
		,"Vendor_Posting_Group"
		,"ITI_Recipient_Bank_Account"
		,"Ship_to_Code"
		,"Ship_to_Name"
		,"Ship_to_Address"
		,"Ship_to_Address_2"
		,"Ship_to_City"
		,"Ship_to_County"
		,"Ship_to_Post_Code"
		,"Ship_to_Country_Region_Code"
		,"Ship_to_Contact"
		,"Applicable_For_Serv_Decl"
		,"Pay_to_Name"
		,"Pay_to_Address"
		,"Pay_to_Address_2"
		,"Pay_to_City"
		,"Pay_to_County"
		,"Pay_to_Post_Code"
		,"Pay_to_Country_Region_Code"
		,"Pay_to_Contact_No"
		,"PayToContactPhoneNo"
		,"PayToContactMobilePhoneNo"
		,"PayToContactEmail"
		,"Pay_to_Contact"
		,"Remit_to_Code"
		,"Remit_to_Name"
		,"Remit_to_Address"
		,"Remit_to_Address_2"
		,"Remit_to_City"
		,"Remit_to_County"
		,"Remit_to_Post_Code"
		,"Remit_to_Country_Region_Code"
		,"Remit_to_Contact"
		,"SystemModifiedAt"
		,"Invoice_Type"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
		$41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
		$61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
		$81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, $93, $94, $95, $96, $97, $98, $99, $100,
		$101, $102  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_No") DO UPDATE
	SET
		"No" = EXCLUDED."No"
		,"Buy_from_Vendor_No" = EXCLUDED."Buy_from_Vendor_No"
		,"Buy_from_Vendor_Name" = EXCLUDED."Buy_from_Vendor_Name"
		,"Buy_from_Address" = EXCLUDED."Buy_from_Address"
		,"Buy_from_Address_2" = EXCLUDED."Buy_from_Address_2"
		,"Buy_from_City" = EXCLUDED."Buy_from_City"
		,"Buy_from_County" = EXCLUDED."Buy_from_County"
		,"Buy_from_Post_Code" = EXCLUDED."Buy_from_Post_Code"
		,"Buy_from_Country_Region_Code" = EXCLUDED."Buy_from_Country_Region_Code"
		,"Buy_from_Contact_No" = EXCLUDED."Buy_from_Contact_No"
		,"BuyFromContactPhoneNo" = EXCLUDED."BuyFromContactPhoneNo"
		,"BuyFromContactMobilePhoneNo" = EXCLUDED."BuyFromContactMobilePhoneNo"
		,"BuyFromContactEmail" = EXCLUDED."BuyFromContactEmail"
		,"Buy_from_Contact" = EXCLUDED."Buy_from_Contact"
		,"ITI_Posting_Desc_Code" = EXCLUDED."ITI_Posting_Desc_Code"
		,"ITI_Posting_Description" = EXCLUDED."ITI_Posting_Description"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"VAT_Reporting_Date" = EXCLUDED."VAT_Reporting_Date"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"ITI_Document_Receipt_Date" = EXCLUDED."ITI_Document_Receipt_Date"
		,"ITI_Rev_Chrg_Purchase_Delivery_Date" = EXCLUDED."ITI_Rev_Chrg_Purchase_Delivery_Date"
		,"ITI_VAT_Settlement_Date" = EXCLUDED."ITI_VAT_Settlement_Date"
		,"ITI_Postponed_VAT" = EXCLUDED."ITI_Postponed_VAT"
		,"ITI_Disable_NSeI_for_Purchase" = EXCLUDED."ITI_Disable_NSeI_for_Purchase"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Quote_No" = EXCLUDED."Quote_No"
		,"Order_No" = EXCLUDED."Order_No"
		,"ITI_SAFT_Ext_Document_No" = EXCLUDED."ITI_SAFT_Ext_Document_No"
		,"ITI_NSeI_Document_No" = EXCLUDED."ITI_NSeI_Document_No"
		,"Vendor_Invoice_No" = EXCLUDED."Vendor_Invoice_No"
		,"Vendor_Order_No" = EXCLUDED."Vendor_Order_No"
		,"Pre_Assigned_No" = EXCLUDED."Pre_Assigned_No"
		,"No_Printed" = EXCLUDED."No_Printed"
		,"Order_Address_Code" = EXCLUDED."Order_Address_Code"
		,"Purchaser_Code" = EXCLUDED."Purchaser_Code"
		,"Responsibility_Center" = EXCLUDED."Responsibility_Center"
		,"Cancelled" = EXCLUDED."Cancelled"
		,"Corrective" = EXCLUDED."Corrective"
		,"EDN_OGL_Declaration" = EXCLUDED."EDN_OGL_Declaration"
		,"EDN_Equivalent_Set" = EXCLUDED."EDN_Equivalent_Set"
		,"EDN_Purchase_Information" = EXCLUDED."EDN_Purchase_Information"
		,"EDN_Container_No" = EXCLUDED."EDN_Container_No"
		,"EDN_Container_Type_Code" = EXCLUDED."EDN_Container_Type_Code"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"EDN_Currency_Date" = EXCLUDED."EDN_Currency_Date"
		,"EDN_Curr_Exchange_Rate" = EXCLUDED."EDN_Curr_Exchange_Rate"
		,"ITI_Split_Payment" = EXCLUDED."ITI_Split_Payment"
		,"EU_3_Party_Trade" = EXCLUDED."EU_3_Party_Trade"
		,"Expected_Receipt_Date" = EXCLUDED."Expected_Receipt_Date"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Payment_Discount_Percent" = EXCLUDED."Payment_Discount_Percent"
		,"Pmt_Discount_Date" = EXCLUDED."Pmt_Discount_Date"
		,"Tax_Liable" = EXCLUDED."Tax_Liable"
		,"Tax_Area_Code" = EXCLUDED."Tax_Area_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Payment_Reference" = EXCLUDED."Payment_Reference"
		,"Creditor_No" = EXCLUDED."Creditor_No"
		,"ITI_VATRegNoOrigCountryCode" = EXCLUDED."ITI_VATRegNoOrigCountryCode"
		,"ITI_VAT_Registration_No" = EXCLUDED."ITI_VAT_Registration_No"
		,"ITI_VAT_Status_Code" = EXCLUDED."ITI_VAT_Status_Code"
		,"Vendor_Posting_Group" = EXCLUDED."Vendor_Posting_Group"
		,"ITI_Recipient_Bank_Account" = EXCLUDED."ITI_Recipient_Bank_Account"
		,"Ship_to_Code" = EXCLUDED."Ship_to_Code"
		,"Ship_to_Name" = EXCLUDED."Ship_to_Name"
		,"Ship_to_Address" = EXCLUDED."Ship_to_Address"
		,"Ship_to_Address_2" = EXCLUDED."Ship_to_Address_2"
		,"Ship_to_City" = EXCLUDED."Ship_to_City"
		,"Ship_to_County" = EXCLUDED."Ship_to_County"
		,"Ship_to_Post_Code" = EXCLUDED."Ship_to_Post_Code"
		,"Ship_to_Country_Region_Code" = EXCLUDED."Ship_to_Country_Region_Code"
		,"Ship_to_Contact" = EXCLUDED."Ship_to_Contact"
		,"Applicable_For_Serv_Decl" = EXCLUDED."Applicable_For_Serv_Decl"
		,"Pay_to_Name" = EXCLUDED."Pay_to_Name"
		,"Pay_to_Address" = EXCLUDED."Pay_to_Address"
		,"Pay_to_Address_2" = EXCLUDED."Pay_to_Address_2"
		,"Pay_to_City" = EXCLUDED."Pay_to_City"
		,"Pay_to_County" = EXCLUDED."Pay_to_County"
		,"Pay_to_Post_Code" = EXCLUDED."Pay_to_Post_Code"
		,"Pay_to_Country_Region_Code" = EXCLUDED."Pay_to_Country_Region_Code"
		,"Pay_to_Contact_No" = EXCLUDED."Pay_to_Contact_No"
		,"PayToContactPhoneNo" = EXCLUDED."PayToContactPhoneNo"
		,"PayToContactMobilePhoneNo" = EXCLUDED."PayToContactMobilePhoneNo"
		,"PayToContactEmail" = EXCLUDED."PayToContactEmail"
		,"Pay_to_Contact" = EXCLUDED."Pay_to_Contact"
		,"Remit_to_Code" = EXCLUDED."Remit_to_Code"
		,"Remit_to_Name" = EXCLUDED."Remit_to_Name"
		,"Remit_to_Address" = EXCLUDED."Remit_to_Address"
		,"Remit_to_Address_2" = EXCLUDED."Remit_to_Address_2"
		,"Remit_to_City" = EXCLUDED."Remit_to_City"
		,"Remit_to_County" = EXCLUDED."Remit_to_County"
		,"Remit_to_Post_Code" = EXCLUDED."Remit_to_Post_Code"
		,"Remit_to_Country_Region_Code" = EXCLUDED."Remit_to_Country_Region_Code"
		,"Remit_to_Contact" = EXCLUDED."Remit_to_Contact"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"Invoice_Type" = EXCLUDED."Invoice_Type"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,concat(litera_firmy,'_', NEW."No")
		,NEW."Buy_from_Vendor_No"
		,NEW."Buy_from_Vendor_Name"
		,NEW."Buy_from_Address"
		,NEW."Buy_from_Address_2"
		,NEW."Buy_from_City"
		,NEW."Buy_from_County"
		,NEW."Buy_from_Post_Code"
		,NEW."Buy_from_Country_Region_Code"
		,NEW."Buy_from_Contact_No"
		,NEW."BuyFromContactPhoneNo"
		,NEW."BuyFromContactMobilePhoneNo"
		,NEW."BuyFromContactEmail"
		,NEW."Buy_from_Contact"
		,NEW."ITI_Posting_Desc_Code"
		,NEW."ITI_Posting_Description"
		,NEW."Posting_Date"
		,NEW."VAT_Reporting_Date"
		,NEW."Document_Date"
		,NEW."ITI_Document_Receipt_Date"
		,NEW."ITI_Rev_Chrg_Purchase_Delivery_Date"
		,NEW."ITI_VAT_Settlement_Date"
		,NEW."ITI_Postponed_VAT"
		,NEW."ITI_Disable_NSeI_for_Purchase"
		,NEW."Due_Date"
		,NEW."Quote_No"
		,NEW."Order_No"
		,NEW."ITI_SAFT_Ext_Document_No"
		,NEW."ITI_NSeI_Document_No"
		,NEW."Vendor_Invoice_No"
		,NEW."Vendor_Order_No"
		,NEW."Pre_Assigned_No"
		,NEW."No_Printed"
		,NEW."Order_Address_Code"
		,NEW."Purchaser_Code"
		,NEW."Responsibility_Center"
		,NEW."Cancelled"
		,NEW."Corrective"
		,NEW."EDN_OGL_Declaration"
		,NEW."EDN_Equivalent_Set"
		,NEW."EDN_Purchase_Information"
		,NEW."EDN_Container_No"
		,NEW."EDN_Container_Type_Code"
		,NEW."Currency_Code"
		,NEW."EDN_Currency_Date"
		,NEW."EDN_Curr_Exchange_Rate"
		,NEW."ITI_Split_Payment"
		,NEW."EU_3_Party_Trade"
		,NEW."Expected_Receipt_Date"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Payment_Discount_Percent"
		,NEW."Pmt_Discount_Date"
		,NEW."Tax_Liable"
		,NEW."Tax_Area_Code"
		,NEW."Location_Code"
		,NEW."Shipment_Method_Code"
		,NEW."Payment_Reference"
		,NEW."Creditor_No"
		,NEW."ITI_VATRegNoOrigCountryCode"
		,NEW."ITI_VAT_Registration_No"
		,NEW."ITI_VAT_Status_Code"
		,NEW."Vendor_Posting_Group"
		,NEW."ITI_Recipient_Bank_Account"
		,NEW."Ship_to_Code"
		,NEW."Ship_to_Name"
		,NEW."Ship_to_Address"
		,NEW."Ship_to_Address_2"
		,NEW."Ship_to_City"
		,NEW."Ship_to_County"
		,NEW."Ship_to_Post_Code"
		,NEW."Ship_to_Country_Region_Code"
		,NEW."Ship_to_Contact"
		,NEW."Applicable_For_Serv_Decl"
		,NEW."Pay_to_Name"
		,NEW."Pay_to_Address"
		,NEW."Pay_to_Address_2"
		,NEW."Pay_to_City"
		,NEW."Pay_to_County"
		,NEW."Pay_to_Post_Code"
		,NEW."Pay_to_Country_Region_Code"
		,NEW."Pay_to_Contact_No"
		,NEW."PayToContactPhoneNo"
		,NEW."PayToContactMobilePhoneNo"
		,NEW."PayToContactEmail"
		,NEW."Pay_to_Contact"
		,NEW."Remit_to_Code"
		,NEW."Remit_to_Name"
		,NEW."Remit_to_Address"
		,NEW."Remit_to_Address_2"
		,NEW."Remit_to_City"
		,NEW."Remit_to_County"
		,NEW."Remit_to_Post_Code"
		,NEW."Remit_to_Country_Region_Code"
		,NEW."Remit_to_Contact"
		,NEW."SystemModifiedAt"
		,'Faktura'
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
	grupa_tabel text := 'posted_purchase_invoice_header'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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