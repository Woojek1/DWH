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
	_tabela := format('bc_warehouse_locations_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Code" text NOT NULL
			,"Key_Code" text PRIMARY KEY
			,"Name" text NOT NULL
			,"Use_As_In_Transit" bool NOT NULL
			,"EDN_Disassembly_Location" bool NOT NULL
			,"EDN_Hidden_Location" bool NOT NULL
			,"Ecomview" bool NOT NULL
			,"Address" text NULL
			,"Address_2" text NULL
			,"Post_Code" text NULL
			,"City" text NULL
			,"Country_Region_Code" text NULL
			,"ShowMap" text NULL
			,"Contact" text NULL
			,"Phone_No" text NULL
			,"Fax_No" text NULL
			,"E_Mail" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Code"
			,"Key_Code"
			,"Name"
			,"Use_As_In_Transit"
			,"EDN_Disassembly_Location"
			,"EDN_Hidden_Location"
			,"Ecomview"
			,"Address"
			,"Address_2"
			,"Post_Code"
			,"City"
			,"Country_Region_Code"
			,"ShowMap"
			,"Contact"
			,"Phone_No"
			,"Fax_No"
			,"E_Mail"
			,"Firma"
			,"load_ts"
		)
		SELECT
			wl."Code"
			,concat(%L, '_', wl."Code")
			,wl."Name"
			,wl."Use_As_In_Transit"
			,wl."EDN_Disassembly_Location"
			,wl."EDN_Hidden_Location"
			,wl."Ecomview"
			,wl."Address"
			,wl."Address_2"
			,wl."Post_Code"
			,wl."City"
			,wl."Country_Region_Code"
			,wl."ShowMap"
			,wl."Contact"
			,wl."Phone_No"
			,wl."Fax_No"
			,wl."E_Mail"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I wl
    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;


--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_warehouse_locations()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_warehouse_locations_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Code"
		,"Key_Code"
		,"Name"
		,"Use_As_In_Transit"
		,"EDN_Disassembly_Location"
		,"EDN_Hidden_Location"
		,"Ecomview"
		,"Address"
		,"Address_2"
		,"Post_Code"
		,"City"
		,"Country_Region_Code"
		,"ShowMap"
		,"Contact"
		,"Phone_No"
		,"Fax_No"
		,"E_Mail"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19 -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_Code") DO UPDATE
	SET
		"Code" = EXCLUDED."Code"
		,"Name" = EXCLUDED."Name"
		,"Use_As_In_Transit" = EXCLUDED."Use_As_In_Transit"
		,"EDN_Disassembly_Location" = EXCLUDED."EDN_Disassembly_Location"
		,"EDN_Hidden_Location" = EXCLUDED."EDN_Hidden_Location"
		,"Ecomview" = EXCLUDED."Ecomview"
		,"Address" = EXCLUDED."Address"
		,"Address_2" = EXCLUDED."Address_2"
		,"Post_Code" = EXCLUDED."Post_Code"
		,"City" = EXCLUDED."City"
		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
		,"ShowMap" = EXCLUDED."ShowMap"
		,"Contact" = EXCLUDED."Contact"
		,"Phone_No" = EXCLUDED."Phone_No"
		,"Fax_No" = EXCLUDED."Fax_No"
		,"E_Mail" = EXCLUDED."E_Mail"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		new."Code"
		,concat(litera_firmy, '_' , new."Code")
		,new."Name"
		,new."Use_As_In_Transit"
		,new."EDN_Disassembly_Location"
		,new."EDN_Hidden_Location"
		,new."Ecomview"
		,new."Address"
		,new."Address_2"
		,new."Post_Code"
		,new."City"
		,new."Country_Region_Code"
		,new."ShowMap"
		,new."Contact"
		,new."Phone_No"
		,new."Fax_No"
		,new."E_Mail"
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
	grupa_tabel text := 'warehouse_locations'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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