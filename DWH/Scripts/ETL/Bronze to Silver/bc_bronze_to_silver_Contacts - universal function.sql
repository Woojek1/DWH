------------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES HEADER TABLES IN SILVER LAYER AND FIRST LOAD
------------------------------------------------------------------------------


DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY['aircon', 'zymetric', 'technab'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_contact_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No_Contact" text NOT NULL PRIMARY key
			,"Key_No_Contact" text NOT NULL
			,"Name" text NULL
			,"Name_2" text NULL
			,"Type" text NULL
			,"Company_No" text NULL
			,"Company_Name" text NULL
			,"Job_Title" text NULL
			,"Contact_Business_Relation" text NULL
			,"IntegrationCustomerNo" text NULL
			,"Search_Name" text NULL
			,"Salesperson_Code" text NULL
			,"Salutation_Code" text NULL
			,"Organizational_Level_Code" text NULL
			,"LastDateTimeModified" timestamptz NULL
			,"Date_of_Last_Interaction" date NULL
			,"Last_Date_Attempted" date NULL
			,"Next_Task_Date" date NULL
			,"Exclude_from_Segment" bool NULL
			,"Privacy_Blocked" bool NULL
			,"Minor" bool NULL
			,"Parental_Consent_Received" bool NULL
			,"Registration_Number" text NULL
			,"Marketing_agreement" bool NULL
			,"MagentoID" int4 NULL
			,"Address" text NULL
			,"Address_2" text NULL
			,"Country_Region_Code" text NULL
			,"Post_Code" text NULL
			,"City" text NULL
			,"EDN_Region_Code" text NULL
			,"EDN_Province_Code" text NULL
			,"ShowMap" text NULL
			,"Phone_No" text NULL
			,"Mobile_Phone_No" text NULL
			,"E_Mail" text NULL
			,"Fax_No" text NULL
			,"Home_Page" text NULL
			,"Correspondence_Type" text NULL
			,"Language_Code" text NULL
			,"Format_Region" text NULL
			,"Currency_Code" text NULL
			,"Territory_Code" text NULL
			,"ITI_VATRegNoOrigCountryCode" text NULL
			,"VAT_Registration_No" text NULL
			,"EDN_NAV_Key" text NULL
			,"SystemModifiedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No_Contact"
			,"Key_No_Contact"
			,"Name"
			,"Name_2"
			,"Type"
			,"Company_No"
			,"Company_Name"
			,"Job_Title"
			,"Contact_Business_Relation"
			,"IntegrationCustomerNo"
			,"Search_Name"
			,"Salesperson_Code"
			,"Salutation_Code"
			,"Organizational_Level_Code"
			,"LastDateTimeModified"
			,"Date_of_Last_Interaction"
			,"Last_Date_Attempted"
			,"Next_Task_Date"
			,"Exclude_from_Segment"
			,"Privacy_Blocked"
			,"Minor"
			,"Parental_Consent_Received"
			,"Registration_Number"
			,"Marketing_agreement"
			,"MagentoID"
			,"Address"
			,"Address_2"
			,"Country_Region_Code"
			,"Post_Code"
			,"City"
			,"EDN_Region_Code"
			,"EDN_Province_Code"
			,"ShowMap"
			,"Phone_No"
			,"Mobile_Phone_No"
			,"E_Mail"
			,"Fax_No"
			,"Home_Page"
			,"Correspondence_Type"
			,"Language_Code"
			,"Format_Region"
			,"Currency_Code"
			,"Territory_Code"
			,"ITI_VATRegNoOrigCountryCode"
			,"VAT_Registration_No"
			,"EDN_NAV_Key"
			,"SystemModifiedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			c."No"
			,concat(%L, ' ' , c."No")
			,c."Name"
			,c."Name_2"
			,c."Type"
			,c."Company_No"
			,c."Company_Name"
			,c."Job_Title"
			,c."Contact_Business_Relation"
			,c."IntegrationCustomerNo"
			,c."Search_Name"
			,c."Salesperson_Code"
			,c."Salutation_Code"
			,c."Organizational_Level_Code"
			,c."LastDateTimeModified"
			,c."Date_of_Last_Interaction"
			,c."Last_Date_Attempted"
			,c."Next_Task_Date"
			,c."Exclude_from_Segment"
			,c."Privacy_Blocked"
			,c."Minor"
			,c."Parental_Consent_Received"
			,c."Registration_Number"
			,c."Marketing_agreement"
			,c."MagentoID"
			,c."Address"
			,c."Address_2"
			,c."Country_Region_Code"
			,c."Post_Code"
			,c."City"
			,c."EDN_Region_Code"
			,c."EDN_Province_Code"
			,c."ShowMap"
			,c."Phone_No"
			,c."Mobile_Phone_No"
			,c."E_Mail"
			,c."Fax_No"
			,c."Home_Page"
			,c."Correspondence_Type"
			,c."Language_Code"
			,c."Format_Region"
			,c."Currency_Code"
			,c."Territory_Code"
			,c."ITI_VATRegNoOrigCountryCode"
			,c."VAT_Registration_No"
			,c."EDN_NAV_Key"
			,c."SystemModifiedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I c
    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_contact()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_contact_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No_Contact"
		,"Key_No_Contact"
		,"Name"
		,"Name_2"
		,"Type"
		,"Company_No"
		,"Company_Name"
		,"Job_Title"
		,"Contact_Business_Relation"
		,"IntegrationCustomerNo"
		,"Search_Name"
		,"Salesperson_Code"
		,"Salutation_Code"
		,"Organizational_Level_Code"
		,"LastDateTimeModified"
		,"Date_of_Last_Interaction"
		,"Last_Date_Attempted"
		,"Next_Task_Date"
		,"Exclude_from_Segment"
		,"Privacy_Blocked"
		,"Minor"
		,"Parental_Consent_Received"
		,"Registration_Number"
		,"Marketing_agreement"
		,"MagentoID"
		,"Address"
		,"Address_2"
		,"Country_Region_Code"
		,"Post_Code"
		,"City"
		,"EDN_Region_Code"
		,"EDN_Province_Code"
		,"ShowMap"
		,"Phone_No"
		,"Mobile_Phone_No"
		,"E_Mail"
		,"Fax_No"
		,"Home_Page"
		,"Correspondence_Type"
		,"Language_Code"
		,"Format_Region"
		,"Currency_Code"
		,"Territory_Code"
		,"ITI_VATRegNoOrigCountryCode"
		,"VAT_Registration_No"
		,"EDN_NAV_Key"
		,"SystemModifiedAt"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
		$41,$42,$43,$44,$45,$46,$47,$48,$49-- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No_Contact") DO UPDATE
	SET
		"Key_No_Contact" = EXCLUDED."Key_No_Contact"
		,"Name" = EXCLUDED."Name"
		,"Name_2" = EXCLUDED."Name_2"
		,"Type" = EXCLUDED."Type"
		,"Company_No" = EXCLUDED."Company_No"
		,"Company_Name" = EXCLUDED."Company_Name"
		,"Job_Title" = EXCLUDED."Job_Title"
		,"Contact_Business_Relation" = EXCLUDED."Contact_Business_Relation"
		,"IntegrationCustomerNo" = EXCLUDED."IntegrationCustomerNo"
		,"Search_Name" = EXCLUDED."Search_Name"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Salutation_Code" = EXCLUDED."Salutation_Code"
		,"Organizational_Level_Code" = EXCLUDED."Organizational_Level_Code"
		,"LastDateTimeModified" = EXCLUDED."LastDateTimeModified"
		,"Date_of_Last_Interaction" = EXCLUDED."Date_of_Last_Interaction"
		,"Last_Date_Attempted" = EXCLUDED."Last_Date_Attempted"
		,"Next_Task_Date" = EXCLUDED."Next_Task_Date"
		,"Exclude_from_Segment" = EXCLUDED."Exclude_from_Segment"
		,"Privacy_Blocked" = EXCLUDED."Privacy_Blocked"
		,"Minor" = EXCLUDED."Minor" 
		,"Parental_Consent_Received" = EXCLUDED."Parental_Consent_Received"
		,"Registration_Number" = EXCLUDED."Registration_Number"
		,"Marketing_agreement" = EXCLUDED."Marketing_agreement"
		,"MagentoID" = EXCLUDED."MagentoID"
		,"Address" = EXCLUDED."Address"
		,"Address_2" = EXCLUDED."Address_2"
		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
		,"Post_Code" = EXCLUDED."Post_Code"
		,"City" = EXCLUDED."City"
		,"EDN_Region_Code" = EXCLUDED."EDN_Region_Code"
		,"EDN_Province_Code" = EXCLUDED."EDN_Province_Code"
		,"ShowMap" = EXCLUDED."ShowMap"
		,"Phone_No" = EXCLUDED."Phone_No"
		,"Mobile_Phone_No" = EXCLUDED."Mobile_Phone_No"
		,"E_Mail" = EXCLUDED."E_Mail"
		,"Fax_No" = EXCLUDED."Fax_No"
		,"Home_Page" = EXCLUDED."Home_Page"
		,"Correspondence_Type" = EXCLUDED."Correspondence_Type"
		,"Language_Code" = EXCLUDED."Language_Code"
		,"Format_Region" = EXCLUDED."Format_Region"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Territory_Code" = EXCLUDED."Territory_Code"
		,"ITI_VATRegNoOrigCountryCode" = EXCLUDED."ITI_VATRegNoOrigCountryCode"
		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		new."No"
		,concat(litera_firmy, ' ' , new."No")
		,new."Name"
		,new."Name_2"
		,new."Type"
		,new."Company_No"
		,new."Company_Name"
		,new."Job_Title"
		,new."Contact_Business_Relation"
		,new."IntegrationCustomerNo"
		,new."Search_Name"
		,new."Salesperson_Code"
		,new."Salutation_Code"
		,new."Organizational_Level_Code"
		,new."LastDateTimeModified"
		,new."Date_of_Last_Interaction"
		,new."Last_Date_Attempted"
		,new."Next_Task_Date"
		,new."Exclude_from_Segment"
		,new."Privacy_Blocked"
		,new."Minor"
		,new."Parental_Consent_Received"
		,new."Registration_Number"
		,new."Marketing_agreement"
		,new."MagentoID"
		,new."Address"
		,new."Address_2"
		,new."Country_Region_Code"
		,new."Post_Code"
		,new."City"
		,new."EDN_Region_Code"
		,new."EDN_Province_Code"
		,new."ShowMap"
		,new."Phone_No"
		,new."Mobile_Phone_No"
		,new."E_Mail"
		,new."Fax_No"
		,new."Home_Page"
		,new."Correspondence_Type"
		,new."Language_Code"
		,new."Format_Region"
		,new."Currency_Code"
		,new."Territory_Code"
		,NEW."ITI_VATRegNoOrigCountryCode"
		,new."VAT_Registration_No"
		,new."EDN_NAV_Key"
		,new."SystemModifiedAt"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



-------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON POSTED SALES INVOICES HEADER TABLE
-------------------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'contact'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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