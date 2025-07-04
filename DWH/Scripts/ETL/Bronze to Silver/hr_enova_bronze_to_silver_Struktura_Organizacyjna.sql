-------------------------------------------------------------------------------
-- CREATING ENOVA STRUKTURA ORGANIZACYJNA TABLES IN SILVER LAYER AND FIRST LOAD
-------------------------------------------------------------------------------
 

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
	_tabela := format('hr_enova_struktura_organizacyjna_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"PracownikID" int4 NOT NULL
			,"PracownikKod" varchar(20) NOT NULL
			,"Pracownik" varchar(255) NOT NULL
			,"StanowiskoKod" varchar(20) NOT NULL
			,"StanowiskoNazwa" varchar(255) NOT NULL
			,"EtatStanowiskoHist" text NULL
			,"EtatMiejscePracyHist" text NULL
			,"PrzelozonyID" int4 NULL
			,"PrzelozonyKod" varchar(20) NULL
			,"Przelozony" varchar(255) NULL
			,"StanowiskoPrzelozKod" varchar(20) NULL
			,"StanowiskoPrzelozNazwa" varchar(255) NULL
			,"CzyManager" bool NULL
			,"LiczbaPodwladnych" int4 NULL
			,"StanowiskoUmZew" varchar NULL
			,"UmZewnetrzna" bool NULL
			,"EmailHR" varchar NULL
			,"EmailAD" varchar NULL
			,"Active" bool NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("PracownikID")
		);
	$ddl$, _tabela, _litera_firmy);


 
-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"PracownikID"
			,"PracownikKod"
			,"Pracownik"
			,"StanowiskoKod"
			,"StanowiskoNazwa"
			,"EtatStanowiskoHist"
			,"EtatMiejscePracyHist"
			,"PrzelozonyID"
			,"PrzelozonyKod"
			,"Przelozony"
			,"StanowiskoPrzelozKod" 
			,"StanowiskoPrzelozNazwa"
			,"CzyManager"
			,"LiczbaPodwladnych"
			,"StanowiskoUmZew"
			,"UmZewnetrzna"
			,"EmailHR"
			,"EmailAD"
			,"Active"
			,"Firma"
			,"load_ts"
		)
		SELECT
			hr."PracownikID"
			,hr."PracownikKod"
			,INITCAP(hr."Pracownik")
			,hr."StanowiskoKod"
			,hr."StanowiskoNazwa"
			,hr."EtatStanowiskoHist"
			,hr."EtatMiejscePracyHist"
			,hr."PrzelozonyID"
			,hr."PrzelozonyKod"
			,INITCAP(hr."Przelozony")
			,hr."StanowiskoPrzelozKod" 
			,hr."StanowiskoPrzelozNazwa"
			,hr."CzyManager"
			,hr."LiczbaPodwladnych"
			,hr."StanowiskoUmZew"
			,hr."UmZewnetrzna"
			,hr."EmailHR"
			,hr."EmailAD"
			,hr."Active"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I as hr


    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_hr_enova_struktura_organizacyjna()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('hr_enova_struktura_organizacyjna_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"PracownikID"
		,"PracownikKod"
		,"Pracownik"
		,"StanowiskoKod"
		,"StanowiskoNazwa"
		,"EtatStanowiskoHist"
		,"EtatMiejscePracyHist"
		,"PrzelozonyID"
		,"PrzelozonyKod"
		,"Przelozony"
		,"StanowiskoPrzelozKod" 
		,"StanowiskoPrzelozNazwa"
		,"CzyManager"
		,"LiczbaPodwladnych"
		,"StanowiskoUmZew"
		,"UmZewnetrzna"
		,"EmailHR"
		,"EmailAD"
		,"Active"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("PracownikID") DO UPDATE
	SET
		"PracownikID" = EXCLUDED."PracownikID"
		,"PracownikKod" = EXCLUDED."PracownikKod"
		,"Pracownik" = EXCLUDED."Pracownik"
		,"StanowiskoKod" = EXCLUDED."StanowiskoKod"
		,"StanowiskoNazwa" = EXCLUDED."StanowiskoNazwa"
		,"EtatStanowiskoHist" = EXCLUDED."EtatStanowiskoHist"
		,"EtatMiejscePracyHist" = EXCLUDED."EtatMiejscePracyHist"
		,"PrzelozonyID" = EXCLUDED."PrzelozonyID"
		,"PrzelozonyKod" = EXCLUDED."PrzelozonyKod"
		,"Przelozony" = EXCLUDED."Przelozony"
		,"StanowiskoPrzelozKod" = EXCLUDED."StanowiskoPrzelozKod"
		,"StanowiskoPrzelozNazwa" = EXCLUDED."StanowiskoPrzelozNazwa"
		,"CzyManager" = EXCLUDED."CzyManager"
		,"LiczbaPodwladnych" = EXCLUDED."LiczbaPodwladnych"
		,"StanowiskoUmZew" = EXCLUDED."StanowiskoUmZew"
		,"UmZewnetrzna" = EXCLUDED."UmZewnetrzna"
		,"EmailHR" = EXCLUDED."EmailHR"
		,"EmailAD" = EXCLUDED."EmailAD"
		,"Active" = EXCLUDED."Active"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."PracownikID"
		,NEW."PracownikKod"
		,INITCAP(NEW."Pracownik")
		,NEW."StanowiskoKod"
		,NEW."StanowiskoNazwa"
		,NEW."EtatStanowiskoHist"
		,NEW."EtatMiejscePracyHist"
		,NEW."PrzelozonyID"
		,NEW."PrzelozonyKod"
		,INITCAP(NEW."Przelozony")
		,NEW."StanowiskoPrzelozKod" 
		,NEW."StanowiskoPrzelozNazwa"
		,NEW."CzyManager"
		,NEW."LiczbaPodwladnych"
		,NEW."StanowiskoUmZew"
		,NEW."UmZewnetrzna"
		,NEW."EmailHR"
		,NEW."EmailAD"
		,NEW."Active"
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
	grupa_tabel text := 'struktura_organizacyjna'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon', 'zymetric', 'technab'];
	firma text;
BEGIN
	FOREACH firma IN ARRAY firmy LOOP
		EXECUTE format($sql$
			DROP TRIGGER IF EXISTS trg_upsert_hr_enova_%I_%I ON bronze.hr_enova_%I_%I;
			CREATE TRIGGER trg_upsert_hr_enova_%I_%I
			AFTER INSERT OR UPDATE ON bronze.hr_enova_%I_%I
			FOR EACH ROW
			EXECUTE FUNCTION bronze.fn_upsert_hr_enova_%I(%L)
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