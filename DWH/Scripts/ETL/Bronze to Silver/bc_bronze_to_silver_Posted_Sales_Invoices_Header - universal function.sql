-----------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES LINES TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------

 
DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY[ 'aircon', 'technab', 'zymetric'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_posted_sales_invoices_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text PRIMARY KEY
			,"Key_No_Invoice" text NULL
			,"Sell_to_Customer_No" text NULL
			,"Sell_to_Customer_Name" text NULL
			,"VAT_Registration_No" text NULL
			,"Sell_to_Address" text NULL
			,"Sell_to_Address_2" text NULL
			,"Sell_to_City" text NULL
			,"Sell_to_County" text NULL
			,"Sell_to_Post_Code" text NULL
			,"Document_Date" date NULL
			,"Posting_Date" date NULL
			,"Due_Date" date NULL
			,"Quote_No" text NULL
			,"Order_No" text NULL
			,"Pre_Assigned_No" text NULL
			,"External_Document_No" text NULL
			,"Salesperson_Code" text NULL	
			,"EDN_Factoring_Invoice" bool NULL
			,"EDN_KUKE_Symbol" text NULL
			,"Remaining_Amount" numeric(14,2) null
			,"Currency_Code" text NULL
			,"Currency_Factor" numeric(38,20) NULL
			,"Shipment_Date" date NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Customer_Posting_Group" text NULL
			,"Location_Code" text NULL
			,"Shipment_Method_Code" text NULL
			,"Shipping_Agent_Code" text NULL
			,"Invoice_Type" text NULL	
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Key_No_Invoice"
			,"Sell_to_Customer_No"
			,"Sell_to_Customer_Name"
			,"VAT_Registration_No"
			,"Sell_to_Address"
			,"Sell_to_Address_2"
			,"Sell_to_City"
			,"Sell_to_County"
			,"Sell_to_Post_Code"
			,"Document_Date"
			,"Posting_Date"
			,"Due_Date"
			,"Quote_No"
			,"Order_No"
			,"Pre_Assigned_No"
			,"External_Document_No"
			,"Salesperson_Code"
			,"EDN_Factoring_Invoice"
			,"EDN_KUKE_Symbol"
			,"Remaining_Amount"
			,"Currency_Code"
			,"Currency_Factor"
			,"Shipment_Date"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Customer_Posting_Group"
			,"Location_Code"
			,"Shipment_Method_Code"
			,"Shipping_Agent_Code"
			,"Invoice_Type"
			,"Firma"
			,"load_ts"
		)
		SELECT
			ih."No"
			,concat(%L, '_', ih."No")
			,ih."Sell_to_Customer_No"
			,ih."Sell_to_Customer_Name"
			,REGEXP_REPLACE(ih."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
			,ih."Sell_to_Address"
			,ih."Sell_to_Address_2"
			,INITCAP(TRIM(ih."Sell_to_City"))
			,LOWER(TRIM(ih."Sell_to_County"))
			,ih."Sell_to_Post_Code"
			,ih."Document_Date"
			,ih."Posting_Date"
			,ih."Due_Date"
			,ih."Quote_No"
			,ih."Order_No"
			,ih."Pre_Assigned_No"
			,ih."External_Document_No"
			,ih."Salesperson_Code"
			,ih."EDN_Factoring_Invoice"
			,ih."EDN_KUKE_Symbol"
			,ih."Remaining_Amount"
			,CASE
				WHEN ih."Currency_Code" = '' THEN 'PLN'
				ELSE ih."Currency_Code"
			END
			,ih."Currency_Factor"
			,ih."Shipment_Date"
			,ih."Payment_Terms_Code"
			,ih."Payment_Method_Code"
			,ih."Shortcut_Dimension_1_Code"
			,ih."Shortcut_Dimension_2_Code"
			,ih."Customer_Posting_Group"
			,ih."Location_Code"
			,ih."Shipment_Method_Code"
			,ih."Shipping_Agent_Code"
			,'Faktura'
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I ih


    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_sales_invoices_header()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_sales_invoices_header_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"Key_No_Invoice"
		,"Sell_to_Customer_No"
		,"Sell_to_Customer_Name"
		,"VAT_Registration_No"
		,"Sell_to_Address"
		,"Sell_to_Address_2"
		,"Sell_to_City"
		,"Sell_to_County"
		,"Sell_to_Post_Code"
		,"Document_Date"
		,"Posting_Date"
		,"Due_Date"
		,"Quote_No"
		,"Order_No"
		,"Pre_Assigned_No"
		,"External_Document_No"
		,"Salesperson_Code"
		,"EDN_Factoring_Invoice"
		,"EDN_KUKE_Symbol"
		,"Remaining_Amount"
		,"Currency_Code"
		,"Currency_Factor"
		,"Shipment_Date"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Customer_Posting_Group"
		,"Location_Code"
		,"Shipment_Method_Code"
		,"Shipping_Agent_Code"
		,"Invoice_Type"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"Key_No_Invoice" = EXCLUDED."Key_No_Invoice"
		,"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
		,"Sell_to_Customer_Name" = EXCLUDED."Sell_to_Customer_Name"
		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
		,"Sell_to_Address" = EXCLUDED."Sell_to_Address"
		,"Sell_to_Address_2" = EXCLUDED."Sell_to_Address_2"
		,"Sell_to_City" = EXCLUDED."Sell_to_City"
		,"Sell_to_County" = EXCLUDED."Sell_to_County"
		,"Sell_to_Post_Code" = EXCLUDED."Sell_to_Post_Code"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Quote_No" = EXCLUDED."Quote_No"
		,"Order_No" = EXCLUDED."Order_No"
		,"Pre_Assigned_No" = EXCLUDED."Pre_Assigned_No"
		,"External_Document_No" = EXCLUDED."External_Document_No"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"EDN_Factoring_Invoice" = EXCLUDED."EDN_Factoring_Invoice"
		,"EDN_KUKE_Symbol" = EXCLUDED."EDN_KUKE_Symbol"
		,"Remaining_Amount" = EXCLUDED."Remaining_Amount"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Currency_Factor" = EXCLUDED."Currency_Factor"
		,"Shipment_Date" = EXCLUDED."Shipment_Date"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Customer_Posting_Group" = EXCLUDED."Customer_Posting_Group"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Shipping_Agent_Code" = EXCLUDED."Shipping_Agent_Code"
		,"Invoice_Type" = EXCLUDED."Invoice_Type"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,concat(litera_firmy, '_', new."No")
		,NEW."Sell_to_Customer_No"
		,NEW."Sell_to_Customer_Name"
		,REGEXP_REPLACE(NEW."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g')
		,NEW."Sell_to_Address"
		,NEW."Sell_to_Address_2"
		,INITCAP(TRIM(NEW."Sell_to_City"))
		,LOWER(TRIM(NEW."Sell_to_County"))
		,NEW."Sell_to_Post_Code"
		,NEW."Document_Date"
		,NEW."Posting_Date"
		,NEW."Due_Date"
		,NEW."Quote_No"
		,NEW."Order_No"
		,NEW."Pre_Assigned_No"
		,NEW."External_Document_No"
		,NEW."Salesperson_Code"
		,NEW."EDN_Factoring_Invoice"
		,NEW."EDN_KUKE_Symbol"
		,NEW."Remaining_Amount"
		,CASE
				WHEN NEW."Currency_Code" = '' THEN 'PLN'
				ELSE NEW."Currency_Code"
		END
		,NEW."Currency_Factor"
		,NEW."Shipment_Date"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Customer_Posting_Group"
		,NEW."Location_Code"
		,NEW."Shipment_Method_Code"
		,NEW."Shipping_Agent_Code"
		,'Faktura'
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
	grupa_tabel text := 'posted_sales_invoices_header'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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