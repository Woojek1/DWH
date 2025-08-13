-------------------------------------------------------------
-- CREATING SALES LINES TABLES IN SILVER LAYER AND FIRST LOAD
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
	_tabela := format('bc_posted_purchase_invoice_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_No" text NOT NULL
			,"Key_Document_No" text NOT NULL
			,"Line_No" int4 NOT NULL
			,"Buy_from_Vendor_No" text NULL
			,"Type" text NULL
			,"No" text NULL
			,"Variant_Code" text NULL
			,"Description" text NULL
			,"Description_2" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"FA_Posting_Type" text NULL
			,"Depreciation_Book_Code" text NULL
			,"Quantity" numeric(14, 2) NULL
			,"Unit_of_Measure_Code" text NULL
			,"Unit_of_Measure" text NULL
			,"Direct_Unit_Cost" numeric(14, 2) NULL
			,"Indirect_Cost_Percent" numeric(6, 2) NULL
			,"Unit_Cost_LCY" numeric(14, 2) NULL
			,"Unit_Price_LCY" numeric(14, 2) NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_Including_VAT" numeric(14, 2) NULL
			,"Line_Discount_Percent" numeric(6, 2) NULL
			,"Line_Discount_Amount" numeric(14, 2) NULL
			,"Allow_Invoice_Disc" bool NULL
			,"Inv_Discount_Amount" numeric(14, 2) NULL
			,"Appl_to_Item_Entry" int4 NULL
			,"Order_No" text NULL
			,"Job_No" text NULL
			,"Insurance_No" text NULL
			,"Depr_until_FA_Posting_Date" bool NULL
			,"Depr_Acquisition_Cost" bool NULL
			,"Budgeted_FA_No" text NULL
			,"SystemModifiedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY("Key_Document_No","Line_No")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_No"
			,"Key_Document_No"
			,"Line_No"
			,"Buy_from_Vendor_No"
			,"Type"
			,"No"
			,"Variant_Code"
			,"Description"
			,"Description_2"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"FA_Posting_Type"
			,"Depreciation_Book_Code" 
			,"Quantity"
			,"Unit_of_Measure_Code"
			,"Unit_of_Measure"
			,"Direct_Unit_Cost"
			,"Indirect_Cost_Percent"
			,"Unit_Cost_LCY"
			,"Unit_Price_LCY"
			,"Amount"
			,"Amount_Including_VAT"
			,"Line_Discount_Percent"
			,"Line_Discount_Amount"
			,"Allow_Invoice_Disc"
			,"Inv_Discount_Amount"
			,"Appl_to_Item_Entry"
			,"Order_No"
			,"Job_No"
			,"Insurance_No"
			,"Depr_until_FA_Posting_Date"
			,"Depr_Acquisition_Cost"
			,"Budgeted_FA_No"
			,"SystemModifiedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pil."Document_No"
			,CONCAT(%L, '_', pil."Document_No")
			,pil."Line_No"
			,pil."Buy_from_Vendor_No"
			,case
				when pil."Type" = 'Item' then 'Towar'
				when pil."Type" = 'Charge (Item)' then 'Towar (Korekta)'
				when pil."Type" = 'Resource' then 'Usługa'
				when pil."Type" = 'G/L Account' then 'Zaliczka'
				else ''
			end as "Type"
			,pil."No"
			,pil."Variant_Code"
			,pil."Description"
			,pil."Description_2"
			,pil."Shortcut_Dimension_1_Code"
			,pil."Shortcut_Dimension_2_Code"
			,pil."FA_Posting_Type"
			,pil."Depreciation_Book_Code" 
			,pil."Quantity"
			,pil."Unit_of_Measure_Code"
			,pil."Unit_of_Measure"
			,pil."Direct_Unit_Cost"
			,pil."Indirect_Cost_Percent"
			,pil."Unit_Cost_LCY"
			,pil."Unit_Price_LCY"
			,pil."Amount"
			,pil."Amount_Including_VAT"
			,pil."Line_Discount_Percent"
			,pil."Line_Discount_Amount"
			,pil."Allow_Invoice_Disc"
			,pil."Inv_Discount_Amount"
			,pil."Appl_to_Item_Entry"
			,pil."Order_No"
			,pil."Job_No"
			,pil."Insurance_No"
			,pil."Depr_until_FA_Posting_Date"
			,pil."Depr_Acquisition_Cost"
			,pil."Budgeted_FA_No"
			,pil."SystemModifiedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pil

    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_purchase_invoice_lines()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_purchase_invoice_lines_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Document_No"
		,"Key_Document_No"
		,"Line_No"
		,"Buy_from_Vendor_No"
		,"Type"
		,"No"
		,"Variant_Code"
		,"Description"
		,"Description_2"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"FA_Posting_Type"
		,"Depreciation_Book_Code" 
		,"Quantity"
		,"Unit_of_Measure_Code"
		,"Unit_of_Measure"
		,"Direct_Unit_Cost"
		,"Indirect_Cost_Percent"
		,"Unit_Cost_LCY"
		,"Unit_Price_LCY"
		,"Amount"
		,"Amount_Including_VAT"
		,"Line_Discount_Percent"
		,"Line_Discount_Amount"
		,"Allow_Invoice_Disc"
		,"Inv_Discount_Amount"
		,"Appl_to_Item_Entry"
		,"Order_No"
		,"Job_No"
		,"Insurance_No"
		,"Depr_until_FA_Posting_Date"
		,"Depr_Acquisition_Cost"
		,"Budgeted_FA_No"
		,"SystemModifiedAt"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_Document_No","Line_No") DO UPDATE
	SET
		"Document_No" = EXCLUDED."Document_No"
		,"Key_Document_No" = EXCLUDED."Key_Document_No"
		,"Line_No" = EXCLUDED."Line_No"
		,"Buy_from_Vendor_No" = EXCLUDED."Buy_from_Vendor_No"
		,"Type" = EXCLUDED."Type"
		,"No" = EXCLUDED."No"
		,"Variant_Code" = EXCLUDED."Variant_Code"
		,"Description" = EXCLUDED."Description" 
		,"Description_2" = EXCLUDED."Description_2" 
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"FA_Posting_Type" = EXCLUDED."FA_Posting_Type"
		,"Depreciation_Book_Code" = EXCLUDED."Depreciation_Book_Code"
		,"Quantity" = EXCLUDED."Quantity"
		,"Unit_of_Measure_Code" = EXCLUDED."Unit_of_Measure_Code"
		,"Unit_of_Measure" = EXCLUDED."Unit_of_Measure"
		,"Direct_Unit_Cost" = EXCLUDED."Direct_Unit_Cost"
		,"Indirect_Cost_Percent" = EXCLUDED."Indirect_Cost_Percent"
		,"Unit_Cost_LCY" = EXCLUDED."Unit_Cost_LCY"
		,"Unit_Price_LCY" = EXCLUDED."Unit_Price_LCY"
		,"Amount" = EXCLUDED."Amount"
		,"Amount_Including_VAT" = EXCLUDED."Amount_Including_VAT"
		,"Line_Discount_Percent" = EXCLUDED."Line_Discount_Percent"
		,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
		,"Allow_Invoice_Disc" = EXCLUDED."Allow_Invoice_Disc"
		,"Inv_Discount_Amount" = EXCLUDED."Inv_Discount_Amount"
		,"Appl_to_Item_Entry" = EXCLUDED."Appl_to_Item_Entry"
		,"Order_No" = EXCLUDED."Order_No"
		,"Job_No" = EXCLUDED."Job_No"
		,"Insurance_No" = EXCLUDED."Insurance_No"
		,"Depr_until_FA_Posting_Date" = EXCLUDED."Depr_until_FA_Posting_Date"
		,"Depr_Acquisition_Cost" = EXCLUDED."Depr_Acquisition_Cost"
		,"Budgeted_FA_No" = EXCLUDED."Budgeted_FA_No"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Document_No"
		,NEW."Key_Document_No"
		,NEW."Line_No"
		,NEW."Buy_from_Vendor_No"
		,case
				when pil."Type" = 'Item' then 'Towar'
				when pil."Type" = 'Charge (Item)' then 'Towar (Korekta)'
				when pil."Type" = 'Resource' then 'Usługa'
				when pil."Type" = 'G/L Account' then 'Zaliczka'
				else ''
		end
		,NEW."No"
		,NEW."Variant_Code"
		,NEW."Description"
		,NEW."Description_2"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."FA_Posting_Type"
		,NEW."Depreciation_Book_Code" 
		,NEW."Quantity"
		,NEW."Unit_of_Measure_Code"
		,NEW."Unit_of_Measure"
		,NEW."Direct_Unit_Cost"
		,NEW."Indirect_Cost_Percent"
		,NEW."Unit_Cost_LCY"
		,NEW."Unit_Price_LCY"
		,NEW."Amount"
		,NEW."Amount_Including_VAT"
		,NEW."Line_Discount_Percent"
		,NEW."Line_Discount_Amount"
		,NEW."Allow_Invoice_Disc"
		,NEW."Inv_Discount_Amount"
		,NEW."Appl_to_Item_Entry"
		,NEW."Order_No"
		,NEW."Job_No"
		,NEW."Insurance_No"
		,NEW."Depr_until_FA_Posting_Date"
		,NEW."Depr_Acquisition_Cost"
		,NEW."Budgeted_FA_No"
		,NEW."SystemModifiedAt"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



--------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON SALES LINES TABLE
--------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'posted_purchase_invoice_lines'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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