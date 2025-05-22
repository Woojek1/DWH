----------------------------------------------------------
-- CREATING PROJECTS TABLES IN SILVER LAYER AND FIRST LOAD
----------------------------------------------------------



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
	_tabela := format('bc_projects_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text PRIMARY KEY
			,"Description" text NULL
			,"Description_2" text NULL
			,"Status" text NULL
			,"Creation_Date" date NULL
			,"Manufacturer_Code" text NULL
			,"City" text NULL
			,"County" text NULL
			,"Object_Type" text NULL
			,"Project_Source" text NULL
			,"Manufacturer" text NULL
			,"Planned_Delivery_Date" date NULL
			,"Project_Account_Manager" text NULL
			,"Salesperson_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
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
			p."No"
			,TRIM(p."Description")
			,TRIM(p."Description_2")
			,REPLACE(p."Status", '0 %%', '0%%')
			,NULLIF(p."Creation_Date", DATE '0001-01-01')
			,p."Manufacturer_Code"
			,INITCAP(TRIM(p."City"))
			,LOWER(TRIM(p."County"))
			,p."Object_Type"
			,p."Project_Source"
			,p."Manufacturer"
			,NULLIF(p."Planned_Delivery_Date", DATE '0001-01-01')
			,p."Project_Account_Manager"
			,p."Salesperson_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I p

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli
--
--		ON CONFLICT ("No") DO UPDATE
--		SET
--			"Description" = EXCLUDED."Description"
--			,"Description_2" = EXCLUDED."Description_2"
--			,"Status" = EXCLUDED."Status"
--			,"Creation_Date" = EXCLUDED."Creation_Date"
--			,"Manufacturer_Code" = EXCLUDED."Manufacturer_Code"
--			,"City" = EXCLUDED."City"
--			,"County" = EXCLUDED."County"
--			,"Object_Type" = EXCLUDED."Object_Type"
--			,"Project_Source" = EXCLUDED."Project_Source"
--			,"Manufacturer" = EXCLUDED."Manufacturer"
--			,"Planned_Delivery_Date" = EXCLUDED."Planned_Delivery_Date"
--			,"Project_Account_Manager" = EXCLUDED."Project_Account_Manager"
--			,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
--			,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

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
