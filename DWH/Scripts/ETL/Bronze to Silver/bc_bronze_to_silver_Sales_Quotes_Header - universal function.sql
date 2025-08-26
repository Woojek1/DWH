-----------------------------------------------------------------------------
-- CREATING SALES QUOTES HEADER TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


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
	_tabela := format('bc_sales_quotes_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NULL
			,"Key_No" text PRIMARY KEY
			,"No" text null
			,"Sell_to_Customer_No" text NULL
			,"Key_Sell_to_Customer_No" text NULL
			,"Sell_to_Customer_Name" text NULL
			,"Sell_to_Address" text NULL
			,"Sell_to_Address_2" text NULL
			,"Sell_to_City" text NULL
			,"Sell_to_County" text NULL
			,"Sell_to_Country_Region_Code" text NULL
			,"Order_Date" date NULL
			,"Document_Date" date NULL
			,"Quote_Valid_Until_Date" date NULL
			,"Due_Date" date NULL
			,"Salesperson_Code" text NULL
			,"Status" text NULL
			,"EDN_WasSent" bool NULL
			,"EDN_KUKE_Symbol" text NULL
			,"Currency_Code" text NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Location_Code" text NULL
			,"EDN_Quote_Type_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"No"
			,"Key_No"
			,"Sell_to_Customer_No"
			,"Key_Sell_to_Customer_No"
			,"Sell_to_Customer_Name"
			,"Sell_to_Address"
			,"Sell_to_Address_2"
			,"Sell_to_City"
			,"Sell_to_County"
			,"Sell_to_Country_Region_Code"
			,"Order_Date"
			,"Document_Date"
			,"Quote_Valid_Until_Date"
			,"Due_Date"
			,"Salesperson_Code"
			,"Status"
			,"EDN_WasSent"
			,"EDN_KUKE_Symbol"
			,"Currency_Code"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Location_Code"
			,"EDN_Quote_Type_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			sqh."Document_Type"
			,sqh."No"
			,CONCAT(%L, '_', sqh."No")
			,sqh."Sell_to_Customer_No"
			,CONCAT(%L, '_', sqh."Sell_to_Customer_No")		
			,sqh."Sell_to_Customer_Name"
			,sqh."Sell_to_Address"
			,sqh."Sell_to_Address_2"
			,sqh."Sell_to_City"
			,sqh."Sell_to_County"
			,sqh."Sell_to_Country_Region_Code"
			,sqh."Order_Date"
			,sqh."Document_Date"
			,sqh."Quote_Valid_Until_Date"
			,sqh."Due_Date"
			,sqh."Salesperson_Code"
			,sqh."Status"
			,sqh."EDN_WasSent"
			,sqh."EDN_KUKE_Symbol"
			,sqh."Currency_Code"
			,sqh."Payment_Terms_Code"
			,sqh."Payment_Method_Code"
			,sqh."Shortcut_Dimension_1_Code"
			,sqh."Shortcut_Dimension_2_Code"
			,sqh."Location_Code"
			,sqh."EDN_Quote_Type_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I sqh

    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_sales_quotes_header()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_sales_quotes_header_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Document_Type"
		,"No"
		,"Key_No"
		,"Sell_to_Customer_No"
		,"Key_Sell_to_Customer_No"
		,"Sell_to_Customer_Name"
		,"Sell_to_Address"
		,"Sell_to_Address_2"
		,"Sell_to_City"
		,"Sell_to_County"
		,"Sell_to_Country_Region_Code"
		,"Order_Date"
		,"Document_Date"
		,"Quote_Valid_Until_Date"
		,"Due_Date"
		,"Salesperson_Code"
		,"Status"
		,"EDN_WasSent"
		,"EDN_KUKE_Symbol"
		,"Currency_Code"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Location_Code"
		,"EDN_Quote_Type_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_No") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"No" = EXCLUDED."No"
		,"Key_No" = EXCLUDED."Key_No"
		,"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
		,"Key_Sell_to_Customer_No" = EXCLUDED."Key_Sell_to_Customer_No"
		,"Sell_to_Customer_Name" = EXCLUDED."Sell_to_Customer_Name"
		,"Sell_to_Address" = EXCLUDED."Sell_to_Address"
		,"Sell_to_Address_2" = EXCLUDED."Sell_to_Address_2"
		,"Sell_to_City" = EXCLUDED."Sell_to_City"
		,"Sell_to_County" = EXCLUDED."Sell_to_County"
		,"Sell_to_Country_Region_Code" = EXCLUDED."Sell_to_Country_Region_Code"
		,"Order_Date" = EXCLUDED."Order_Date"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Quote_Valid_Until_Date" = EXCLUDED."Quote_Valid_Until_Date"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Status" = EXCLUDED."Status"
		,"EDN_WasSent" = EXCLUDED."EDN_WasSent"
		,"EDN_KUKE_Symbol" = EXCLUDED."EDN_KUKE_Symbol"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"EDN_Quote_Type_Code" = EXCLUDED."EDN_Quote_Type_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	
	USING
		NEW."Document_Type"
		,NEW."No"
		,CONCAT(litera_firmy, '_', NEW."No")
		,NEW."Sell_to_Customer_No"
		,CONCAT(litera_firmy, '_', NEW."Sell_to_Customer_No")
		,NEW."Sell_to_Customer_Name"
		,NEW."Sell_to_Address"
		,NEW."Sell_to_Address_2"
		,NEW."Sell_to_City"
		,NEW."Sell_to_County"
		,NEW."Sell_to_Country_Region_Code"
		,NEW."Order_Date"
		,NEW."Document_Date"
		,NEW."Quote_Valid_Until_Date"
		,NEW."Due_Date"
		,NEW."Salesperson_Code"
		,NEW."Status"
		,NEW."EDN_WasSent"
		,NEW."EDN_KUKE_Symbol"
		,NEW."Currency_Code"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Location_Code"
		,NEW."EDN_Quote_Type_Code"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;




----------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON SALES QUOTES HEADER TABLE
----------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'sales_quotes_header'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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