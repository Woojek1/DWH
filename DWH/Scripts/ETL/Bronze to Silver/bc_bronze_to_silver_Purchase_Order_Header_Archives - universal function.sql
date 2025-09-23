-----------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES LINES TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------

 
DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY['aircon', 'technab', 'zymetric'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_purchase_order_header_archives_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NULL
			,"No" text NOT NULL
			,"Key_No" text NOT NULL
			,"Doc_No_Occurrence" int4 NOT NULL
			,"Version_No" int4 NOT NULL
			,"Date_Archived" date NULL
			,"Time_Archived" time NULL
			,"Archived_By" text NULL
			,"Interaction_Exist" bool NULL
			,"Buy_from_Vendor_No" text NULL
			,"Key_Buy_from_Vendor_No" text NULL
			,"Order_Address_Code" text NULL
			,"Buy_from_Vendor_Name" text NULL
			,"Vendor_Authorization_No" text NULL
			,"Buy_from_Post_Code" text NULL
			,"Buy_from_Country_Region_Code" text NULL
			,"Buy_from_Contact" text NULL
			,"Pay_to_Vendor_No" text NULL
			,"Pay_to_Name" text NULL
			,"Pay_to_Post_Code" text NULL
			,"Pay_to_Country_Region_Code" text NULL
			,"Pay_to_Contact" text NULL
			,"Ship_to_Code" text NULL
			,"Ship_to_Name" text NULL
			,"Ship_to_Post_Code" text NULL
			,"Ship_to_Country_Region_Code" text NULL
			,"Ship_to_Contact" text NULL
			,"Posting_Date" date NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Location_Code" text NULL
			,"Purchaser_Code" text NULL
			,"Currency_Code" text NULL
			,"Document_Date" date NULL
			,"Payment_Terms_Code" text NULL
			,"Due_Date" date NULL
			,"Payment_Discount_Percent" numeric(10, 2) NULL
			,"Payment_Method_Code" text NULL
			,"Shipment_Method_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("No", "Doc_No_Occurrence", "Version_No")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"No"
			,"Key_No"
			,"Doc_No_Occurrence"
			,"Version_No"
			,"Date_Archived"
			,"Time_Archived"
			,"Archived_By"
			,"Interaction_Exist"
			,"Buy_from_Vendor_No"
			,"Key_Buy_from_Vendor_No"
			,"Order_Address_Code"
			,"Buy_from_Vendor_Name"
			,"Vendor_Authorization_No"
			,"Buy_from_Post_Code"
			,"Buy_from_Country_Region_Code"
			,"Buy_from_Contact"
			,"Pay_to_Vendor_No"
			,"Pay_to_Name"
			,"Pay_to_Post_Code"
			,"Pay_to_Country_Region_Code"
			,"Pay_to_Contact"
			,"Ship_to_Code"
			,"Ship_to_Name"
			,"Ship_to_Post_Code"
			,"Ship_to_Country_Region_Code"
			,"Ship_to_Contact"
			,"Posting_Date"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Location_Code"
			,"Purchaser_Code"
			,"Currency_Code"
			,"Document_Date"
			,"Payment_Terms_Code"
			,"Due_Date"
			,"Payment_Discount_Percent"
			,"Payment_Method_Code"
			,"Shipment_Method_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			poha."Document_Type"
			,poha."No"
			,concat(%L, '_', poha."No")
			,poha."Doc_No_Occurrence"
			,poha."Version_No"
			,poha."Date_Archived"
			,poha."Time_Archived"
			,poha."Archived_By"
			,poha."Interaction_Exist"
			,poha."Buy_from_Vendor_No"
			,concat(%L, '_', poha."Buy_from_Vendor_No")
			,poha."Order_Address_Code"
			,poha."Buy_from_Vendor_Name"
			,poha."Vendor_Authorization_No"
			,poha."Buy_from_Post_Code"
			,poha."Buy_from_Country_Region_Code"
			,poha."Buy_from_Contact"
			,poha."Pay_to_Vendor_No"
			,poha."Pay_to_Name"
			,poha."Pay_to_Post_Code"
			,poha."Pay_to_Country_Region_Code"
			,poha."Pay_to_Contact"
			,poha."Ship_to_Code"
			,poha."Ship_to_Name"
			,poha."Ship_to_Post_Code"
			,poha."Ship_to_Country_Region_Code"
			,poha."Ship_to_Contact"
			,poha."Posting_Date"
			,poha."Shortcut_Dimension_1_Code"
			,poha."Shortcut_Dimension_2_Code"
			,poha."Location_Code"
			,poha."Purchaser_Code"
			,poha."Currency_Code"
			,poha."Document_Date"
			,poha."Payment_Terms_Code"
			,poha."Due_Date"
			,poha."Payment_Discount_Percent"
			,poha."Payment_Method_Code"
			,poha."Shipment_Method_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I poha


    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_purchase_order_header_archives()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_purchase_order_header_archives_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
			"Document_Type"
			,"No"
			,"Key_No"
			,"Doc_No_Occurrence"
			,"Version_No"
			,"Date_Archived"
			,"Time_Archived"
			,"Archived_By"
			,"Interaction_Exist"
			,"Buy_from_Vendor_No"
			,"Key_Buy_from_Vendor_No"
			,"Order_Address_Code"
			,"Buy_from_Vendor_Name"
			,"Vendor_Authorization_No"
			,"Buy_from_Post_Code"
			,"Buy_from_Country_Region_Code"
			,"Buy_from_Contact"
			,"Pay_to_Vendor_No"
			,"Pay_to_Name"
			,"Pay_to_Post_Code"
			,"Pay_to_Country_Region_Code"
			,"Pay_to_Contact"
			,"Ship_to_Code"
			,"Ship_to_Name"
			,"Ship_to_Post_Code"
			,"Ship_to_Country_Region_Code"
			,"Ship_to_Contact"
			,"Posting_Date"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Location_Code"
			,"Purchaser_Code"
			,"Currency_Code"
			,"Document_Date"
			,"Payment_Terms_Code"
			,"Due_Date"
			,"Payment_Discount_Percent"
			,"Payment_Method_Code"
			,"Shipment_Method_Code"
			,"Firma"
			,"load_ts"
	)
	SELECT 
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
		$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
		$41   -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No", "Doc_No_Occurrence", "Version_No") DO UPDATE
	SET
		"Document_Type" = EXCLUDED."Document_Type"
		,"No" = EXCLUDED."No"
		,"Key_No" = EXCLUDED."Key_No"
		,"Doc_No_Occurrence" = EXCLUDED."Doc_No_Occurrence"
		,"Version_No" = EXCLUDED."Version_No"
		,"Date_Archived" = EXCLUDED."Date_Archived"
		,"Time_Archived" = EXCLUDED."Time_Archived"
		,"Archived_By" = EXCLUDED."Archived_By"
		,"Interaction_Exist" = EXCLUDED."Interaction_Exist"
		,"Buy_from_Vendor_No" = EXCLUDED."Buy_from_Vendor_No"
		,"Key_Buy_from_Vendor_No" = EXCLUDED."Key_Buy_from_Vendor_No"
		,"Order_Address_Code" = EXCLUDED."Order_Address_Code"
		,"Buy_from_Vendor_Name" = EXCLUDED."Buy_from_Vendor_Name"
		,"Vendor_Authorization_No" = EXCLUDED."Vendor_Authorization_No"
		,"Buy_from_Post_Code" = EXCLUDED."Buy_from_Post_Code"
		,"Buy_from_Country_Region_Code" = EXCLUDED."Buy_from_Country_Region_Code"
		,"Buy_from_Contact" = EXCLUDED."Buy_from_Contact"
		,"Pay_to_Vendor_No" = EXCLUDED."Pay_to_Vendor_No"
		,"Pay_to_Name" = EXCLUDED."Pay_to_Name"
		,"Pay_to_Post_Code" = EXCLUDED."Pay_to_Post_Code"
		,"Pay_to_Country_Region_Code" = EXCLUDED."Pay_to_Country_Region_Code"
		,"Pay_to_Contact" = EXCLUDED."Pay_to_Contact"
		,"Ship_to_Code" = EXCLUDED."Ship_to_Code"
		,"Ship_to_Name" = EXCLUDED."Ship_to_Name"
		,"Ship_to_Post_Code" = EXCLUDED."Ship_to_Post_Code"
		,"Ship_to_Country_Region_Code" = EXCLUDED."Ship_to_Country_Region_Code"
		,"Ship_to_Contact" = EXCLUDED."Ship_to_Contact"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Purchaser_Code" = EXCLUDED."Purchaser_Code"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"Payment_Discount_Percent" = EXCLUDED."Payment_Discount_Percent"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Document_Type"
		,NEW."No"
		,concat(litera_firmy, '_', NEW."No")
		,NEW."Doc_No_Occurrence"
		,NEW."Version_No"
		,NEW."Date_Archived"
		,NEW."Time_Archived"
		,NEW."Archived_By"
		,NEW."Interaction_Exist"
		,NEW."Buy_from_Vendor_No"
		,concat(litera_firmy, '_', NEW."Buy_from_Vendor_No")
		,NEW."Order_Address_Code"
		,NEW."Buy_from_Vendor_Name"
		,NEW."Vendor_Authorization_No"
		,NEW."Buy_from_Post_Code"
		,NEW."Buy_from_Country_Region_Code"
		,NEW."Buy_from_Contact"
		,NEW."Pay_to_Vendor_No"
		,NEW."Pay_to_Name"
		,NEW."Pay_to_Post_Code"
		,NEW."Pay_to_Country_Region_Code"
		,NEW."Pay_to_Contact"
		,NEW."Ship_to_Code"
		,NEW."Ship_to_Name"
		,NEW."Ship_to_Post_Code"
		,NEW."Ship_to_Country_Region_Code"
		,NEW."Ship_to_Contact"
		,NEW."Posting_Date"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Location_Code"
		,NEW."Purchaser_Code"
		,NEW."Currency_Code"
		,NEW."Document_Date"
		,NEW."Payment_Terms_Code"
		,NEW."Due_Date"
		,NEW."Payment_Discount_Percent"
		,NEW."Payment_Method_Code"
		,NEW."Shipment_Method_Code"
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
	grupa_tabel text := 'purchase_order_header_archives'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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