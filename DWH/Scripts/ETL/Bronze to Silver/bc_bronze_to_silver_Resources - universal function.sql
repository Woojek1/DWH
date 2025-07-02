-----------------------------------------------------------
-- CREATING RESOURCES TABLES IN SILVER LAYER AND FIRST LOAD
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
	_tabela := format('bc_resources_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---

 
-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text NOT NULL PRIMARY KEY
			,"Key_No" text NOT NULL
			,"Name" text NULL
			,"Name_2" text NULL
			,"Type" text NULL
			,"Base_Unit_of_Measure" text NULL
			,"Search_Name" text NULL
			,"Resource_Group_No" text NULL
			,"Blocked" bool NULL
			,"Privacy_Blocked" bool NULL
			,"Last_Date_Modified" date NULL
			,"Use_Time_Sheet" bool NULL
			,"Time_Sheet_Owner_User_ID" text NULL
			,"Time_Sheet_Approver_User_ID" text NULL
			,"Service_Transaction_Type_Code" text NULL
			,"Exclude_From_Service_Decl" bool NULL
			,"EDN_Web_Resource" bool NULL
			,"EDN_NAV_Key" text NULL
			,"Direct_Unit_Cost" numeric(14, 2) NULL
			,"Indirect_Cost_Percent" numeric(6, 2) NULL
			,"Unit_Cost" numeric(14, 2) NULL
			,"Price_Profit_Calculation" text NULL
			,"Profit_Percent" numeric(6, 2) NULL
			,"Unit_Price" numeric(14, 2) NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"VAT_Prod_Posting_Group" text NULL
			,"Default_Deferral_Template_Code" text NULL
			,"Automatic_Ext_Texts" bool NULL
			,"IC_Partner_Purch_G_L_Acc_No" text NULL
			,"ITI_Service_Statistical_Code" text NULL
			,"Job_Title" text NULL
			,"Address" text NULL
			,"Address_2" text NULL
			,"City" text NULL
			,"County" text NULL
			,"Post_Code" text NULL
			,"Country_Region_Code" text NULL
			,"Social_Security_No" text NULL
			,"Education" text NULL
			,"Contract_Class" text NULL
			,"Employment_Date" date NULL
			,"SystemModifiedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL			
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Key_No"
			,"Name"
			,"Name_2"
			,"Type"
			,"Base_Unit_of_Measure"
			,"Search_Name"
			,"Resource_Group_No"
			,"Blocked"
			,"Privacy_Blocked"
			,"Last_Date_Modified"
			,"Use_Time_Sheet"
			,"Time_Sheet_Owner_User_ID"
			,"Time_Sheet_Approver_User_ID"
			,"Service_Transaction_Type_Code"
			,"Exclude_From_Service_Decl"
			,"EDN_Web_Resource"
			,"EDN_NAV_Key"
			,"Direct_Unit_Cost"
			,"Indirect_Cost_Percent"
			,"Unit_Cost"
			,"Price_Profit_Calculation"
			,"Profit_Percent"
			,"Unit_Price"
			,"Gen_Prod_Posting_Group"
			,"VAT_Prod_Posting_Group"
			,"Default_Deferral_Template_Code"
			,"Automatic_Ext_Texts"
			,"IC_Partner_Purch_G_L_Acc_No"
			,"ITI_Service_Statistical_Code"
			,"Job_Title"
			,"Address"
			,"Address_2"
			,"City"
			,"County"
			,"Post_Code"
			,"Country_Region_Code"
			,"Social_Security_No"
			,"Education"
			,"Contract_Class"
			,"Employment_Date"
			,"SystemModifiedAt"
			,"Firma" 
			,"load_ts"
		)
		SELECT
			r."No"
			,CONCAT(%L, '_', r."No")
			,r."Name"
			,r."Name_2"
			,r."Type"
			,r."Base_Unit_of_Measure"
			,r."Search_Name"
			,r."Resource_Group_No"
			,r."Blocked"
			,r."Privacy_Blocked"
			,r."Last_Date_Modified"
			,r."Use_Time_Sheet"
			,r."Time_Sheet_Owner_User_ID"
			,r."Time_Sheet_Approver_User_ID"
			,r."Service_Transaction_Type_Code"
			,r."Exclude_From_Service_Decl"
			,r."EDN_Web_Resource"
			,r."EDN_NAV_Key"
			,r."Direct_Unit_Cost"
			,r."Indirect_Cost_Percent"
			,r."Unit_Cost"
			,r."Price_Profit_Calculation"
			,r."Profit_Percent"
			,r."Unit_Price"
			,r."Gen_Prod_Posting_Group"
			,r."VAT_Prod_Posting_Group"
			,r."Default_Deferral_Template_Code"
			,r."Automatic_Ext_Texts"
			,r."IC_Partner_Purch_G_L_Acc_No"
			,r."ITI_Service_Statistical_Code"
			,r."Job_Title"
			,r."Address"
			,r."Address_2"
			,r."City"
			,r."County"
			,r."Post_Code"
			,r."Country_Region_Code"
			,r."Social_Security_No"
			,r."Education"
			,r."Contract_Class"
			,r."Employment_Date"
			,r."SystemModifiedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I r

    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;
 



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_projects()
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
	target_table := format('bc_projects_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"Description"
		,"Description_2"
		,"Status"
		,"Creation_Date"
		,"Manufacturer_Code"
		,"City"
		,"County"
		,"Object_Type"
		,"Project_Source"
		,"Manufacturer"
		,"Planned_Delivery_Date"
		,"Project_Account_Manager"
		,"Salesperson_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej

	ON CONFLICT("No") DO UPDATE
	SET
		"No" = EXCLUDED."No"
		,"Description" = EXCLUDED."Description"
		,"Description_2" = EXCLUDED."Description_2"
		,"Status" = EXCLUDED."Status"
		,"Creation_Date" = EXCLUDED."Creation_Date"
		,"Manufacturer_Code" = EXCLUDED."Manufacturer_Code"
		,"City" = EXCLUDED."City"
		,"County" = EXCLUDED."County"
		,"Object_Type" = EXCLUDED."Object_Type"
		,"Project_Source" = EXCLUDED."Project_Source"
		,"Manufacturer" = EXCLUDED."Manufacturer"
		,"Planned_Delivery_Date" = EXCLUDED."Planned_Delivery_Date"
		,"Project_Account_Manager" = EXCLUDED."Project_Account_Manager"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,TRIM(NEW."Description")
		,TRIM(NEW."Description_2")
		,REPLACE(NEW."Status", '0 %', '0%')
		,NULLIF(NEW."Creation_Date", DATE '0001-01-01')
		,NEW."Manufacturer_Code"
		,INITCAP(TRIM(NEW."City"))
		,LOWER(TRIM(NEW."County"))
		,NEW."Object_Type"
		,NEW."Project_Source"
		,NEW."Manufacturer"
		,NULLIF(NEW."Planned_Delivery_Date", DATE '0001-01-01')
		,NEW."Project_Account_Manager"
		,NEW."Salesperson_Code"
		,litera_firmy
		,CURRENT_TIMESTAMP;
RETURN NEW;
END;
$function$;



-----------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON PROJECTS TABLE
-----------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'projects'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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
