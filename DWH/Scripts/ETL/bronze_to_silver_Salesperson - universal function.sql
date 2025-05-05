-------------------------------------------------------------
-- CREATING SALESPERSON TABLES IN SILVER LAYER AND FIRST LOAD
-------------------------------------------------------------


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
	_tabela := format('bc_salesperson_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Code" text NULL
			,"Name" text NULL
			,"Job_Title" text NULL
			,"EDN_Supervisor_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Code"
			,"Name"
			,"Job_Title"
			,"EDN_Supervisor_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			"Code"
			,"Name"
			,"Job_Title"
			,"EDN_Supervisor_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I s

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("Code") DO UPDATE
--		SET
--			"Name" = EXCLUDED."Name"
--			,"Job_Title" = EXCLUDED.
--			,"EDN_Supervisor_Code" = EXCLUDED."EDN_Supervisor_Code"
--			,"Firma" = EXCLUDED."Firma"		
--			,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_salesperson()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_salesperson_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Code"
		,"Name"
		,"Job_Title"
		,"EDN_Supervisor_Code"
		,"Firma"
		,"load_ts"
		)
	SELECT 
		$1,$2,$3,$4,$5,$6  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"Code" = EXCLUDED."Code"
		,"Name" = EXCLUDED."Name"
		,"Job_Title" = EXCLUDED.
		,"EDN_Supervisor_Code" = EXCLUDED."EDN_Supervisor_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = EXCLUDED."load_ts";
	$etl$, target_table)
	USING
		NEW."Name"
		,NEW."Job_Title"
		,NEW."EDN_Supervisor_Code"
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
	grupa_tabel text := 'salesperson'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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