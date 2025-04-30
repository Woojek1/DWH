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
			"No" text PRIMARY KEY
			,"Name" text NULL
			,"Name_2" text NULL
			,"Last_Date_Modified" date NULL
			,"Balance_LCY" numeric(14, 2) NULL
			,"Balance_Due_LCY" numeric(14, 2) NULL
			,"Search_Name" text NULL
			,"IC_Partner_Code" text NULL
			,"EDN_NAV_Key" text NULL
			,"Address" text NULL
			,"Country_Region_Code" text NULL
			,"City" text NULL
			,"County" text NULL
			,"Post_Code" text NULL
			,"VAT_Registration_No" text NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Vendor_Posting_Group" text NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Name"
			,"Name_2"
			,"Last_Date_Modified"
			,"Balance_LCY"
			,"Balance_Due_LCY"
			,"Search_Name"
			,"IC_Partner_Code"
			,"EDN_NAV_Key"
			,"Address"
			,"Country_Region_Code"
			,"City"
			,"County"
			,"Post_Code"
			,"VAT_Registration_No"
			,"Gen_Bus_Posting_Group"
			,"Vendor_Posting_Group"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			v."No"
			,v."Name"
			,v."Name_2"
			,v."Last_Date_Modified"
			,v."Balance_LCY"
			,v."Balance_Due_LCY"
			,v."Search_Name"
			,v."IC_Partner_Code"
			,v."EDN_NAV_Key"
			,v."Address"
			,v."Country_Region_Code"
			,INITCAP(TRIM(v."City"))
			,LOWER(TRIM(v."County"))
			,v."Post_Code"
			,REGEXP_REPLACE(v."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
			,v."Gen_Bus_Posting_Group"
			,v."Vendor_Posting_Group"
			,v."Payment_Terms_Code"
			,v."Payment_Method_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I v

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("No") DO UPDATE
--		SET
--		"Name" = EXCLUDED."Name"
--		,"Name_2" = EXCLUDED."Name_2"
--		,"Last_Date_Modified" = EXCLUDED."Last_Date_Modified"
--		,"Balance_LCY" = EXCLUDED."Balance_LCY"
--		,"Balance_Due_LCY" = EXCLUDED."Balance_Due_LCY"
--		,"Search_Name" = EXCLUDED."Search_Name"
--		,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
--		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
--		,"Address" = EXCLUDED."Address"
--		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
--		,"City" = EXCLUDED."City"
--		,"County" = EXCLUDED."County"
--		,"Post_Code" = EXCLUDED."Post_Code"
--		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
--		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
--		,"Vendor_Posting_Group" = EXCLUDED."Vendor_Posting_Group"
--		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
--		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
--		,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

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
		"No"
		,"Name"
		,"Name_2"
		,"Last_Date_Modified"
		,"Balance_LCY"
		,"Balance_Due_LCY"
		,"Search_Name"
		,"IC_Partner_Code"
		,"EDN_NAV_Key"
		,"Address"
		,"Country_Region_Code"
		,"City"
		,"County"
		,"Post_Code"
		,"VAT_Registration_No"
		,"Gen_Bus_Posting_Group"
		,"Vendor_Posting_Group"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"Name" = EXCLUDED."Name"
		,"Name_2" = EXCLUDED."Name_2"
		,"Last_Date_Modified" = EXCLUDED."Last_Date_Modified"
		,"Balance_LCY" = EXCLUDED."Balance_LCY"
		,"Balance_Due_LCY" = EXCLUDED."Balance_Due_LCY"
		,"Search_Name" = EXCLUDED."Search_Name"
		,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
		,"Address" = EXCLUDED."Address"
		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
		,"City" = EXCLUDED."City"
		,"County" = EXCLUDED."County"
		,"Post_Code" = EXCLUDED."Post_Code"
		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Vendor_Posting_Group" = EXCLUDED."Vendor_Posting_Group"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = EXCLUDED."load_ts";
	$etl$, target_table)
	USING
		NEW."No"
		,NEW."Name"
		,NEW."Name_2"
		,NEW."Last_Date_Modified"
		,NEW."Balance_LCY"
		,NEW."Balance_Due_LCY"
		,NEW."Search_Name"
		,NEW."IC_Partner_Code"
		,NEW."EDN_NAV_Key"
		,NEW."Address"
		,NEW."Country_Region_Code"
		,INITCAP(TRIM(NEW."City"))
		,LOWER(TRIM(NEW."County"))
		,NEW."Post_Code"
		,REGEXP_REPLACE(NEW."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g')
		,NEW."Gen_Bus_Posting_Group"
		,NEW."Vendor_Posting_Group"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
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