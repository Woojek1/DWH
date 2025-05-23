-----------------------------------------------------------------------------
-- CREATING SALES ORDERS HEADER TABLES IN SILVER LAYER AND FIRST LOAD
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
	_tabela := format('bc_sales_orders_header_archive_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NULL
			,"No" text PRIMARY KEY
			,"Sell_to_Customer_No" text NULL
			,"Sell_to_Customer_Name" text NULL
			,"Quote_No" text NULL
			,"Sell_to_Address" text NULL
			,"Sell_to_Address_2" text NULL
			,"Sell_to_City" text NULL
			,"Sell_to_County" text NULL
			,"Sell_to_Country_Region_Code" text NULL
			,"Posting_Date" date NULL
			,"Order_Date" date NULL
			,"Due_Date" date NULL
			,"Salesperson_Code" text NULL
			,"Status" text NULL
			,"OrderStatus" text NULL
			,"EDN_KUKE_Symbol" text NULL
			,"Currency_Code" text NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Location_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"No"
			,"Sell_to_Customer_No"
			,"Sell_to_Customer_Name"
			,"Quote_No"
			,"Sell_to_Address"
			,"Sell_to_Address_2"
			,"Sell_to_City"
			,"Sell_to_County"
			,"Sell_to_Country_Region_Code"
			,"Posting_Date"
			,"Order_Date"
			,"Due_Date"
			,"Salesperson_Code"
			,"Status"
			,"OrderStatus"
			,"EDN_KUKE_Symbol"
			,"Currency_Code"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Location_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			soh."Document_Type"
			,soh."No"
			,soh."Sell_to_Customer_No"
			,soh."Sell_to_Customer_Name"
			,soh."Quote_No"
			,soh."Sell_to_Address"
			,soh."Sell_to_Address_2"
			,soh."Sell_to_City"
			,soh."Sell_to_County"
			,soh."Sell_to_Country_Region_Code"
			,soh."Posting_Date"
			,soh."Order_Date"
			,soh."Due_Date"
			,soh."Salesperson_Code"
			,soh."Status"
			,soh."OrderStatus"
			,soh."EDN_KUKE_Symbol"
			,soh."Currency_Code"
			,soh."Payment_Terms_Code"
			,soh."Payment_Method_Code"
			,soh."Shortcut_Dimension_1_Code"
			,soh."Shortcut_Dimension_2_Code"
			,soh."Location_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I soh

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("No") DO UPDATE
--		SET
--			"Document_Type" = EXCLUDED."Document_Type"
--			,"No" = EXCLUDED."No"
--			,"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
--			,"Sell_to_Customer_Name" = EXCLUDED."Sell_to_Customer_Name"
--			,"Quote_No" = EXCLUDED."Quote_No"
--			,"Sell_to_Address" = EXCLUDED."Sell_to_Address"
--			,"Sell_to_Address_2" = EXCLUDED."Sell_to_Address_2"
--			,"Sell_to_City" = EXCLUDED."Sell_to_City"
--			,"Sell_to_County" = EXCLUDED."Sell_to_County"
--			,"Sell_to_Country_Region_Code" = EXCLUDED."Sell_to_Country_Region_Code"
--			,"Posting_Date" = EXCLUDED."Posting_Date"
--			,"Order_Date" = EXCLUDED."Order_Date"
--			,"Due_Date" = EXCLUDED."Due_Date"
--			,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
--			,"Status" = EXCLUDED."Status"
--			,"OrderStatus" = EXCLUDED."OrderStatus"
--			,"EDN_KUKE_Symbol" = EXCLUDED."EDN_KUKE_Symbol"
--			,"Currency_Code" = EXCLUDED."Currency_Code"
--			,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
--			,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
--			,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
--			,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
--			,"Location_Code" = EXCLUDED."Location_Code"
--			,"Firma" = EXCLUDED."Firma"
--			,"load_ts" = EXCLUDED."load_ts";
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_sales_orders_header_archive()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_sales_orders_header_archive_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Document_Type"
		,"No"
		,"Sell_to_Customer_No"
		,"Sell_to_Customer_Name"
		,"Quote_No"
		,"Sell_to_Address"
		,"Sell_to_Address_2"
		,"Sell_to_City"
		,"Sell_to_County"
		,"Sell_to_Country_Region_Code"
		,"Posting_Date"
		,"Order_Date"
		,"Due_Date"
		,"Salesperson_Code"
		,"Status"
		,"OrderStatus"
		,"EDN_KUKE_Symbol"
		,"Currency_Code"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Location_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25 -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"No" = EXCLUDED."No"
		,"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
		,"Sell_to_Customer_Name" = EXCLUDED."Sell_to_Customer_Name"
		,"Quote_No" = EXCLUDED."Quote_No"
		,"Sell_to_Address" = EXCLUDED."Sell_to_Address"
		,"Sell_to_Address_2" = EXCLUDED."Sell_to_Address_2"
		,"Sell_to_City" = EXCLUDED."Sell_to_City"
		,"Sell_to_County" = EXCLUDED."Sell_to_County"
		,"Sell_to_Country_Region_Code" = EXCLUDED."Sell_to_Country_Region_Code"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Order_Date" = EXCLUDED."Order_Date"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Status" = EXCLUDED."Status"
		,"OrderStatus" = EXCLUDED."OrderStatus"
		,"EDN_KUKE_Symbol" = EXCLUDED."EDN_KUKE_Symbol"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	
	USING
		NEW."Document_Type"
		,NEW."No"
		,NEW."Sell_to_Customer_No"
		,NEW."Sell_to_Customer_Name"
		,NEW."Quote_No"
		,NEW."Sell_to_Address"
		,NEW."Sell_to_Address_2"
		,NEW."Sell_to_City"
		,NEW."Sell_to_County"
		,NEW."Sell_to_Country_Region_Code"
		,NEW."Posting_Date"
		,NEW."Order_Date"
		,NEW."Due_Date"
		,NEW."Salesperson_Code"
		,NEW."Status"
		,NEW."OrderStatus"
		,NEW."EDN_KUKE_Symbol"
		,NEW."Currency_Code"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Location_Code"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;




----------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON SALES ORDERS HEADER TABLE
----------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'sales_orders_header_archive'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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