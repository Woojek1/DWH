-----------------------------------------------------------
-- CREATING CUSTOMERS TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------


DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY[ 'aircon', 'zymetric', 'technab'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_vendors_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No_Vendor" text NOT NULL PRIMARY KEY
			,"Key_No_Vendor" text NULL
			,"Name" text NULL 
			,"Name_2" text NULL
			,"Blocked" text NULL
			,"Privacy_Blocked" bool NULL
			,"Last_Date_Modified" date NULL
			,"Balance_LCY" numeric(14, 2) NULL
			,"BalanceAsCustomer" numeric(14, 2) NULL
			,"Balance_Due_LCY" numeric(14, 2) NULL
			,"Document_Sending_Profile" text NULL
			,"Search_Name" text NULL
			,"IC_Partner_Code" text NULL
			,"Purchaser_Code" text NULL
			,"Responsibility_Center" text NULL
			,"Disable_Search_by_Name" bool NULL
			,"ITI_Disable_NSeI_for_Purchase" bool NULL
			,"Company_Size_Code" text NULL
			,"EDN_Contract_Vendor" bool NULL
			,"EDN_NAV_Key" text NULL
			,"Address" text NULL
			,"Address_2" text NULL
			,"Country_Region_Code" text NULL
			,"City" text NULL
			,"County" text NULL
			,"Post_Code" text NULL
			,"ShowMap" text NULL
			,"Phone_No" text NULL
			,"MobilePhoneNo" text NULL
			,"E_Mail" text NULL
			,"Fax_No" text NULL
			,"Home_Page" text NULL
			,"Our_Account_No" text NULL
			,"Language_Code" text NULL
			,"Format_Region" text NULL
			,"Primary_Contact_No" text NULL
			,"Control16" text NULL
			,"ITI_VATRegNoOrigCountryCode" text NULL
			,"VAT_Registration_No" text NULL
			,"ITI_Regon_No" text NULL
			,"ITI_KRS_No" text NULL
			,"ITI_Registration_Authority" text NULL
			,"EORI_Number" text NULL
			,"GLN" text NULL
			,"Tax_Liable" bool NULL
			,"Tax_Area_Code" text NULL
			,"Pay_to_Vendor_No" text NULL
			,"Invoice_Disc_Code" text NULL
			,"Prices_Including_VAT" bool NULL
			,"ITI_Ext_Doc_No_Def_Part" text NULL
			,"Price_Calculation_Method" text NULL
			,"Registration_Number" text NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"VAT_Bus_Posting_Group" text NULL
			,"Vendor_Posting_Group" text NULL
			,"Allow_Multiple_Posting_Groups" bool NULL
			,"Currency_Code" text NULL
			,"Prepayment_Percent" numeric(6, 2) NULL
			,"Application_Method" text NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Priority" int4 NULL
			,"Block_Payment_Tolerance" bool NULL
			,"Preferred_Bank_Account_Code" text NULL
			,"Partner_Type" text NULL
			,"Intrastat_Partner_Type" text NULL
			,"Cash_Flow_Payment_Terms_Code" text NULL
			,"Creditor_No" text NULL
			,"Exclude_from_Pmt_Practices" bool NULL
			,"Location_Code" text NULL
			,"Shipment_Method_Code" text NULL
			,"Lead_Time_Calculation" text NULL
			,"Base_Calendar_Code" text NULL
			,"Customized_Calendar" text NULL
			,"Over_Receipt_Code" text NULL
			,"Default_Trans_Type" text NULL
			,"Default_Trans_Type_Return" text NULL
			,"Def_Transport_Method" text NULL
			,"SystemModifiedAt" timestamptz NULL
			,"Global_Dimension_1_Filter" text NULL
			,"Global_Dimension_2_Filter" text NULL
			,"Currency_Filter" text NULL
			,"SystemCreatedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No_Vendor"
			,"Key_No_Vendor"
			,"Name"
			,"Name_2"
			,"Blocked"
			,"Privacy_Blocked"
			,"Last_Date_Modified"
			,"Balance_LCY"
			,"BalanceAsCustomer"
			,"Balance_Due_LCY"
			,"Document_Sending_Profile"
			,"Search_Name"
			,"IC_Partner_Code"
			,"Purchaser_Code"
			,"Responsibility_Center"
			,"Disable_Search_by_Name"
			,"ITI_Disable_NSeI_for_Purchase"
			,"Company_Size_Code"
			,"EDN_Contract_Vendor"
			,"EDN_NAV_Key"
			,"Address"
			,"Address_2"
			,"Country_Region_Code"
			,"City"
			,"County"
			,"Post_Code"
			,"ShowMap"
			,"Phone_No"
			,"MobilePhoneNo"
			,"E_Mail"
			,"Fax_No"
			,"Home_Page"
			,"Our_Account_No"
			,"Language_Code"
			,"Format_Region"
			,"Primary_Contact_No"
			,"Control16"
			,"ITI_VATRegNoOrigCountryCode"
			,"VAT_Registration_No"
			,"ITI_Regon_No"
			,"ITI_KRS_No"
			,"ITI_Registration_Authority"
			,"EORI_Number"
			,"GLN"
			,"Tax_Liable"
			,"Tax_Area_Code"
			,"Pay_to_Vendor_No"
			,"Invoice_Disc_Code"
			,"Prices_Including_VAT"
			,"ITI_Ext_Doc_No_Def_Part"
			,"Price_Calculation_Method"
			,"Registration_Number"
			,"Gen_Bus_Posting_Group"
			,"VAT_Bus_Posting_Group"
			,"Vendor_Posting_Group"
			,"Allow_Multiple_Posting_Groups"
			,"Currency_Code"
			,"Prepayment_Percent"
			,"Application_Method"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Priority"
			,"Block_Payment_Tolerance"
			,"Preferred_Bank_Account_Code" 
			,"Partner_Type"
			,"Intrastat_Partner_Type"
			,"Cash_Flow_Payment_Terms_Code"
			,"Creditor_No"
			,"Exclude_from_Pmt_Practices"
			,"Location_Code"
			,"Shipment_Method_Code"
			,"Lead_Time_Calculation"
			,"Base_Calendar_Code"
			,"Customized_Calendar"
			,"Over_Receipt_Code"
			,"Default_Trans_Type"
			,"Default_Trans_Type_Return"
			,"Def_Transport_Method"
			,"SystemModifiedAt"
			,"Global_Dimension_1_Filter"
			,"Global_Dimension_2_Filter"
			,"Currency_Filter"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			v."No"
			,concat(%L, '_', v."No")
			,v."Name"
			,v."Name_2"
			,v."Blocked"
			,v."Privacy_Blocked"
			,v."Last_Date_Modified"
			,v."Balance_LCY"
			,v."BalanceAsCustomer"
			,v."Balance_Due_LCY"
			,v."Document_Sending_Profile"
			,v."Search_Name"
			,v."IC_Partner_Code"
			,v."Purchaser_Code"
			,v."Responsibility_Center"
			,v."Disable_Search_by_Name"
			,v."ITI_Disable_NSeI_for_Purchase"
			,v."Company_Size_Code"
			,v."EDN_Contract_Vendor"
			,v."EDN_NAV_Key"
			,v."Address"
			,v."Address_2"
			,v."Country_Region_Code"
			,v."City"
			,v."County"
			,v."Post_Code"
			,v."ShowMap"
			,v."Phone_No"
			,v."MobilePhoneNo"
			,v."E_Mail"
			,v."Fax_No"
			,v."Home_Page"
			,v."Our_Account_No"
			,v."Language_Code"
			,v."Format_Region"
			,v."Primary_Contact_No"
			,v."Control16"
			,v."ITI_VATRegNoOrigCountryCode"
			,REGEXP_REPLACE(v."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g')
			,v."ITI_Regon_No"
			,v."ITI_KRS_No"
			,v."ITI_Registration_Authority"
			,v."EORI_Number"
			,v."GLN"
			,v."Tax_Liable"
			,v."Tax_Area_Code"
			,v."Pay_to_Vendor_No"
			,v."Invoice_Disc_Code"
			,v."Prices_Including_VAT"
			,v."ITI_Ext_Doc_No_Def_Part"
			,v."Price_Calculation_Method"
			,v."Registration_Number"
			,v."Gen_Bus_Posting_Group"
			,v."VAT_Bus_Posting_Group"
			,v."Vendor_Posting_Group"
			,v."Allow_Multiple_Posting_Groups"
			,v."Currency_Code"
			,v."Prepayment_Percent"
			,v."Application_Method"
			,v."Payment_Terms_Code"
			,v."Payment_Method_Code"
			,v."Priority"
			,v."Block_Payment_Tolerance"
			,v."Preferred_Bank_Account_Code" 
			,v."Partner_Type"
			,v."Intrastat_Partner_Type"
			,v."Cash_Flow_Payment_Terms_Code"
			,v."Creditor_No"
			,v."Exclude_from_Pmt_Practices"
			,v."Location_Code"
			,v."Shipment_Method_Code"
			,v."Lead_Time_Calculation"
			,v."Base_Calendar_Code"
			,v."Customized_Calendar"
			,v."Over_Receipt_Code"
			,v."Default_Trans_Type"
			,v."Default_Trans_Type_Return"
			,v."Def_Transport_Method"
			,v."SystemModifiedAt"
			,v."Global_Dimension_1_Filter"
			,v."Global_Dimension_2_Filter"
			,v."Currency_Filter"
			,v."SystemCreatedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I v


    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_vendors()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_vendors_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
			"No_Vendor"
			,"Key_No_Vendor"
			,"Name"
			,"Name_2"
			,"Blocked"
			,"Privacy_Blocked"
			,"Last_Date_Modified"
			,"Balance_LCY"
			,"BalanceAsCustomer"
			,"Balance_Due_LCY"
			,"Document_Sending_Profile"
			,"Search_Name"
			,"IC_Partner_Code"
			,"Purchaser_Code"
			,"Responsibility_Center"
			,"Disable_Search_by_Name"
			,"ITI_Disable_NSeI_for_Purchase"
			,"Company_Size_Code"
			,"EDN_Contract_Vendor"
			,"EDN_NAV_Key"
			,"Address"
			,"Address_2"
			,"Country_Region_Code"
			,"City"
			,"County"
			,"Post_Code"
			,"ShowMap"
			,"Phone_No"
			,"MobilePhoneNo"
			,"E_Mail"
			,"Fax_No"
			,"Home_Page"
			,"Our_Account_No"
			,"Language_Code"
			,"Format_Region"
			,"Primary_Contact_No"
			,"Control16"
			,"ITI_VATRegNoOrigCountryCode"
			,"VAT_Registration_No"
			,"ITI_Regon_No"
			,"ITI_KRS_No"
			,"ITI_Registration_Authority"
			,"EORI_Number"
			,"GLN"
			,"Tax_Liable"
			,"Tax_Area_Code"
			,"Pay_to_Vendor_No"
			,"Invoice_Disc_Code"
			,"Prices_Including_VAT"
			,"ITI_Ext_Doc_No_Def_Part"
			,"Price_Calculation_Method"
			,"Registration_Number"
			,"Gen_Bus_Posting_Group"
			,"VAT_Bus_Posting_Group"
			,"Vendor_Posting_Group"
			,"Allow_Multiple_Posting_Groups"
			,"Currency_Code"
			,"Prepayment_Percent"
			,"Application_Method"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Priority"
			,"Block_Payment_Tolerance"
			,"Preferred_Bank_Account_Code" 
			,"Partner_Type"
			,"Intrastat_Partner_Type"
			,"Cash_Flow_Payment_Terms_Code"
			,"Creditor_No"
			,"Exclude_from_Pmt_Practices"
			,"Location_Code"
			,"Shipment_Method_Code"
			,"Lead_Time_Calculation"
			,"Base_Calendar_Code"
			,"Customized_Calendar"
			,"Over_Receipt_Code"
			,"Default_Trans_Type"
			,"Default_Trans_Type_Return"
			,"Def_Transport_Method"
			,"SystemModifiedAt"
			,"Global_Dimension_1_Filter"
			,"Global_Dimension_2_Filter"
			,"Currency_Filter"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
	)
	SELECT 
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
		$41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
		$61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
		$81, $82, $83, $84, $85
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No_Vendor") DO UPDATE
	SET
		"Key_No_Vendor" = EXCLUDED."Key_No_Vendor"
		,"Name" = EXCLUDED."Name"
		,"Name_2" = EXCLUDED."Name_2"
		,"Blocked" = EXCLUDED."Blocked"
		,"Privacy_Blocked" = EXCLUDED."Privacy_Blocked"
		,"Last_Date_Modified" = EXCLUDED."Last_Date_Modified"
		,"Balance_LCY" = EXCLUDED."Balance_LCY"
		,"BalanceAsCustomer" = EXCLUDED."BalanceAsCustomer"
		,"Balance_Due_LCY" = EXCLUDED."Balance_Due_LCY"
		,"Document_Sending_Profile" = EXCLUDED."Document_Sending_Profile"
		,"Search_Name" = EXCLUDED."Search_Name"
		,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
		,"Purchaser_Code" = EXCLUDED."Purchaser_Code"
		,"Responsibility_Center" = EXCLUDED."Responsibility_Center"
		,"Disable_Search_by_Name" = EXCLUDED."Disable_Search_by_Name"
		,"ITI_Disable_NSeI_for_Purchase" = EXCLUDED."ITI_Disable_NSeI_for_Purchase"
		,"Company_Size_Code" = EXCLUDED."Company_Size_Code"
		,"EDN_Contract_Vendor" = EXCLUDED."EDN_Contract_Vendor"
		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
		,"Address" = EXCLUDED."Address"
		,"Address_2" = EXCLUDED."Address_2"
		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
		,"City" = EXCLUDED."City"
		,"County" = EXCLUDED."County"
		,"Post_Code" = EXCLUDED."Post_Code"
		,"ShowMap" = EXCLUDED."ShowMap"
		,"Phone_No" = EXCLUDED."Phone_No"
		,"MobilePhoneNo" = EXCLUDED."MobilePhoneNo"
		,"E_Mail" = EXCLUDED."E_Mail"
		,"Fax_No" = EXCLUDED."Fax_No"
		,"Home_Page" = EXCLUDED."Home_Page"
		,"Our_Account_No" = EXCLUDED."Our_Account_No"
		,"Language_Code" = EXCLUDED."Language_Code"
		,"Format_Region" = EXCLUDED."Format_Region"
		,"Primary_Contact_No" = EXCLUDED."Primary_Contact_No"
		,"Control16" = EXCLUDED."Control16"
		,"ITI_VATRegNoOrigCountryCode" = EXCLUDED."ITI_VATRegNoOrigCountryCode"
		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
		,"ITI_Regon_No" = EXCLUDED."ITI_Regon_No" 
		,"ITI_KRS_No" = EXCLUDED."ITI_KRS_No"
		,"ITI_Registration_Authority" = EXCLUDED."ITI_Registration_Authority"
		,"EORI_Number" = EXCLUDED."EORI_Number"
		,"GLN" = EXCLUDED."GLN"
		,"Tax_Liable" = EXCLUDED."Tax_Liable"
		,"Tax_Area_Code" = EXCLUDED."Tax_Area_Code"
		,"Pay_to_Vendor_No" = EXCLUDED."Pay_to_Vendor_No"
		,"Invoice_Disc_Code" = EXCLUDED."Invoice_Disc_Code"
		,"Prices_Including_VAT" = EXCLUDED."Prices_Including_VAT"
		,"ITI_Ext_Doc_No_Def_Part" = EXCLUDED."ITI_Ext_Doc_No_Def_Part"
		,"Price_Calculation_Method" = EXCLUDED."Price_Calculation_Method"
		,"Registration_Number" = EXCLUDED."Registration_Number"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"VAT_Bus_Posting_Group" = EXCLUDED."VAT_Bus_Posting_Group"
		,"Vendor_Posting_Group" = EXCLUDED."Vendor_Posting_Group"
		,"Allow_Multiple_Posting_Groups" = EXCLUDED."Allow_Multiple_Posting_Groups"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Prepayment_Percent" = EXCLUDED."Prepayment_Percent"
		,"Application_Method" = EXCLUDED."Application_Method"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Priority" = EXCLUDED."Priority"
		,"Block_Payment_Tolerance" = EXCLUDED."Block_Payment_Tolerance"
		,"Preferred_Bank_Account_Code" = EXCLUDED."Preferred_Bank_Account_Code"
		,"Partner_Type" = EXCLUDED."Partner_Type"
		,"Intrastat_Partner_Type" = EXCLUDED."Intrastat_Partner_Type"
		,"Cash_Flow_Payment_Terms_Code" = EXCLUDED."Cash_Flow_Payment_Terms_Code"
		,"Creditor_No" = EXCLUDED."Creditor_No"
		,"Exclude_from_Pmt_Practices" = EXCLUDED."Exclude_from_Pmt_Practices"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Lead_Time_Calculation" = EXCLUDED."Lead_Time_Calculation"
		,"Base_Calendar_Code" = EXCLUDED."Base_Calendar_Code"
		,"Customized_Calendar" = EXCLUDED."Customized_Calendar"
		,"Over_Receipt_Code" = EXCLUDED."Over_Receipt_Code"
		,"Default_Trans_Type" = EXCLUDED."Default_Trans_Type"
		,"Default_Trans_Type_Return" = EXCLUDED."Default_Trans_Type_Return"
		,"Def_Transport_Method" = EXCLUDED."Def_Transport_Method"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"Global_Dimension_1_Filter" = EXCLUDED."Global_Dimension_1_Filter"
		,"Global_Dimension_2_Filter" = EXCLUDED."Global_Dimension_2_Filter"
		,"Currency_Filter" = EXCLUDED."Currency_Filter"
		,"SystemCreatedAt" = EXCLUDED."SystemCreatedAt"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
$etl$, target_table)
	USING
		new."No"
		,concat(litera_firmy, '_', new."No")
		,new."Name"
		,new."Name_2"
		,new."Blocked"
		,new."Privacy_Blocked"
		,new."Last_Date_Modified"
		,new."Balance_LCY"
		,new."BalanceAsCustomer"
		,new."Balance_Due_LCY"
		,new."Document_Sending_Profile"
		,new."Search_Name"
		,new."IC_Partner_Code"
		,new."Purchaser_Code"
		,new."Responsibility_Center"
		,new."Disable_Search_by_Name"
		,new."ITI_Disable_NSeI_for_Purchase"
		,new."Company_Size_Code"
		,new."EDN_Contract_Vendor"
		,new."EDN_NAV_Key"
		,new."Address"
		,new."Address_2"
		,new."Country_Region_Code"
		,new."City"
		,new."County"
		,new."Post_Code"
		,new."ShowMap"
		,new."Phone_No"
		,new."MobilePhoneNo"
		,new."E_Mail"
		,new."Fax_No"
		,new."Home_Page"
		,new."Our_Account_No"
		,new."Language_Code"
		,new."Format_Region"
		,new."Primary_Contact_No"
		,new."Control16"
		,new."ITI_VATRegNoOrigCountryCode"
		,REGEXP_REPLACE(new."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g')
		,new."ITI_Regon_No"
		,new."ITI_KRS_No"
		,new."ITI_Registration_Authority"
		,new."EORI_Number"
		,new."GLN"
		,new."Tax_Liable"
		,new."Tax_Area_Code"
		,new."Pay_to_Vendor_No"
		,new."Invoice_Disc_Code"
		,new."Prices_Including_VAT"
		,new."ITI_Ext_Doc_No_Def_Part"
		,new."Price_Calculation_Method"
		,new."Registration_Number"
		,new."Gen_Bus_Posting_Group"
		,new."VAT_Bus_Posting_Group"
		,new."Vendor_Posting_Group"
		,new."Allow_Multiple_Posting_Groups"
		,new."Currency_Code"
		,new."Prepayment_Percent"
		,new."Application_Method"
		,new."Payment_Terms_Code"
		,new."Payment_Method_Code"
		,new."Priority"
		,new."Block_Payment_Tolerance"
		,new."Preferred_Bank_Account_Code" 
		,new."Partner_Type"
		,new."Intrastat_Partner_Type"
		,new."Cash_Flow_Payment_Terms_Code"
		,new."Creditor_No"
		,new."Exclude_from_Pmt_Practices"
		,new."Location_Code"
		,new."Shipment_Method_Code"
		,new."Lead_Time_Calculation"
		,new."Base_Calendar_Code"
		,new."Customized_Calendar"
		,new."Over_Receipt_Code"
		,new."Default_Trans_Type"
		,new."Default_Trans_Type_Return"
		,new."Def_Transport_Method"
		,new."SystemModifiedAt"
		,new."Global_Dimension_1_Filter"
		,new."Global_Dimension_2_Filter"
		,new."Currency_Filter"
		,new."SystemCreatedAt"
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
	grupa_tabel text := 'vendors'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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
		grupa_tabel,          -- dla funkcji fn_upsert_bc
		firma                 -- parametr do funkcji jako tekst
		);
	END LOOP;
END;
$$ LANGUAGE plpgsql;