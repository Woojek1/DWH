--------------------------------------------------------------------------------
-- CREATING POSTED SALES CREDIT MEMO LINES TABLES IN SILVER LAYER AND FIRST LOAD
--------------------------------------------------------------------------------


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
	_tabela := format('bc_posted_sales_credit_memo_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_No" text NOT NULL
			,"Line_No" int4 NOT NULL
			,"Sell_to_Customer_No" text NULL
			,"Type" text NULL
			,"No" text NULL
			,"Description" text NULL
			,"Description_2" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Quantity" numeric(14, 2) NULL
			,"Unit_of_Measure" text NULL
			,"Unit_Price" numeric(14, 2) NULL
			,"Unit_Cost_LCY" numeric(14, 5) NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_Including_VAT" numeric(14, 2) NULL
			,"Line_Discount_Percent" numeric(6, 2) NULL
			,"Line_Discount_Amount" numeric(14, 2) NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("Document_No", "Line_No")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_No"
			,"Line_No"
			,"Sell_to_Customer_No"
			,"Type"
			,"No"
			,"Description"
			,"Description_2"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Quantity"
			,"Unit_of_Measure"
			,"Unit_Price"
			,"Unit_Cost_LCY"
			,"Amount"
			,"Amount_Including_VAT"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Firma"
			,"load_ts"
		)
		SELECT
			"Document_No"
			,cm."Line_No"
			,cm."Sell_to_Customer_No"
			,cm."Type"
			,cm."No"
			,cm."Description"
			,cm."Description_2"
			,cm."Shortcut_Dimension_1_Code"
			,cm."Shortcut_Dimension_2_Code"
			,cm."Quantity"
			,cm."Unit_of_Measure"
			,cm."Unit_Price"
			,cm."Unit_Cost_LCY"
			,cm."Amount"
			,cm."Amount_Including_VAT"
			,cm."Line_Discount_Percent"
			,cm."Line_Discount_Amount"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I cm

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT (""Document_No", "Line_No") DO UPDATE
--		SET
--		"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
--		,"Type" = EXCLUDED."Type"
--		,"No" = EXCLUDED."No"
--		,"Description" = EXCLUDED."Description"
--		,"Description_2" = EXCLUDED."Description_2"
--		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
--		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
--		,"Quantity" = EXCLUDED."Quantity"
--		,"Unit_of_Measure" = EXCLUDED."Unit_of_Measure"
--		,"Unit_Price" = EXCLUDED."Unit_Price"
--		,"Unit_Cost_LCY" = EXCLUDED."Unit_Cost_LCY"
--		,"Amount" = EXCLUDED."Amount"
--		,"Amount_Including_VAT" = EXCLUDED."Amount_Including_VAT"
--		,"Line_Discount_Percent" = EXCLUDED."Line_Discount_Percent"
--		,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
--		,"Firma" = EXCLUDED."Firma"
--		,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_sales_credit_memo_lines()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_sales_credit_memo_lines_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Document_No"
		,"Line_No"
		,"Sell_to_Customer_No"
		,"Type"
		,"No"
		,"Description"
		,"Description_2"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Quantity"
		,"Unit_of_Measure"
		,"Unit_Price"
		,"Unit_Cost_LCY"
		,"Amount"
		,"Amount_Including_VAT"
		,"Line_Discount_Percent"
		,"Line_Discount_Amount"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Document_No", "Line_No") DO UPDATE
	SET
		"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
		,"Type" = EXCLUDED."Type"
		,"No" = EXCLUDED."No"
		,"Description" = EXCLUDED."Description"
		,"Description_2" = EXCLUDED."Description_2"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Quantity" = EXCLUDED."Quantity"
		,"Unit_of_Measure" = EXCLUDED."Unit_of_Measure"
		,"Unit_Price" = EXCLUDED."Unit_Price"
		,"Unit_Cost_LCY" = EXCLUDED."Unit_Cost_LCY"
		,"Amount" = EXCLUDED."Amount"
		,"Amount_Including_VAT" = EXCLUDED."Amount_Including_VAT"
		,"Line_Discount_Percent" = EXCLUDED."Line_Discount_Percent"
		,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Document_No"
        ,NEW."Line_No"
		,NEW."Sell_to_Customer_No"
    	,NEW."Type"
	    ,NEW."No"
	    ,NEW."Description"
	    ,NEW."Description_2"
	    ,NEW."Shortcut_Dimension_1_Code"
	    ,NEW."Shortcut_Dimension_2_Code"
	    ,NEW."Quantity"
	    ,NEW."Unit_of_Measure"
	    ,NEW."Unit_Price"
	    ,NEW."Unit_Cost_LCY"
	    ,NEW."Amount"
	    ,NEW."Amount_Including_VAT"
	    ,NEW."Line_Discount_Percent"
	    ,NEW."Line_Discount_Amount"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



---------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON POSTED SALES CREDIT MEMO LINES TABLE
---------------------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'posted_sales_credit_memo_lines'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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