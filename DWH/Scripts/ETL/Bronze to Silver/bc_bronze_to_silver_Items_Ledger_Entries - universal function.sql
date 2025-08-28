----------------------------------------------------------------------
-- CREATING ITEMS LEDGER ENTRIES TABLES IN SILVER LAYER AND FIRST LOAD
----------------------------------------------------------------------


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
	_tabela := format('bc_items_ledger_entries_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Entry_No" int4 PRIMARY KEY
			,"Posting_Date" date NULL
			,"Entry_Type" text NULL
			,"Document_Type" text NULL
			,"Document_No" text NULL
			,"Document_Line_No" int4 NULL
			,"Item_No" text NULL
			,"Key_Item_No" text NULL
			,"Base_Group" text NULL
			,"EDN_Source_Type" text NULL
			,"EDN_Source_No" text NULL
			,"EDN_Contractor_Name" text NULL
			,"Global_Dimension_1_Code" text NULL
			,"Global_Dimension_2_Code" text NULL
			,"Serial_No" text NULL
			,"Location_Code" text NULL
			,"Quantity" numeric(14,2) NULL
			,"Invoiced_Quantity" numeric(14,2) NULL
			,"Remaining_Quantity" numeric(14,2) NULL
			,"Shipped_Qty_Not_Returned" numeric(14,2) NULL
			,"Reserved_Quantity" numeric(14,2) NULL
			,"Sales_Amount_Expected" numeric(14,2) NULL
			,"Sales_Amount_Actual" numeric(14,2) NULL
			,"Cost_Amount_Expected" numeric(14,2) NULL
			,"Cost_Amount_Actual" numeric(14,2) NULL
			,"Completely_Invoiced" bool NULL
			,"Open" bool NULL
			,"Order_Line_No" int4 NULL
			,"Prod_Order_Comp_Line_No" int4 NULL
			,"Dimension_Set_ID" int4 NULL
			,"Shortcut_Dimension_3_Code" text NULL
			,"Shortcut_Dimension_4_Code" text NULL
			,"Shortcut_Dimension_5_Code" text NULL
			,"Shortcut_Dimension_6_Code" text NULL
			,"Shortcut_Dimension_7_Code" text NULL
			,"Shortcut_Dimension_8_Code" text NULL
			,"EDN_Campaign_No" text NULL
			,"EDN_Campaign_Description" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			
    );
$ddl$, _tabela, _litera_firmy);	

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Entry_No"
			,"Posting_Date"
			,"Entry_Type"
			,"Document_Type"
			,"Document_No"
			,"Document_Line_No"
			,"Item_No"
			,"Key_Item_No"
			,"Base_Group"
			,"EDN_Source_Type"
			,"EDN_Source_No"
			,"EDN_Contractor_Name"
			,"Global_Dimension_1_Code"
			,"Global_Dimension_2_Code"
			,"Serial_No"
			,"Location_Code"
			,"Quantity"
			,"Invoiced_Quantity"
			,"Remaining_Quantity"
			,"Shipped_Qty_Not_Returned"
			,"Reserved_Quantity"
			,"Sales_Amount_Expected"
			,"Sales_Amount_Actual"
			,"Cost_Amount_Expected"
			,"Cost_Amount_Actual"
			,"Completely_Invoiced"
			,"Open"
			,"Order_Line_No"
			,"Prod_Order_Comp_Line_No"
			,"Dimension_Set_ID"
			,"Shortcut_Dimension_3_Code"
			,"Shortcut_Dimension_4_Code"
			,"Shortcut_Dimension_5_Code"
			,"Shortcut_Dimension_6_Code"
			,"Shortcut_Dimension_7_Code"
			,"Shortcut_Dimension_8_Code"
			,"EDN_Campaign_No"
			,"EDN_Campaign_Description"
			,"Firma"
			,"load_ts"
		)
		SELECT
			ile."Entry_No"
			,ile."Posting_Date"
			,ile."Entry_Type"
			,ile."Document_Type"
			,ile."Document_No"
			,ile."Document_Line_No"
			,ile."Item_No"
			,CONCAT(%L, '_', ile."Item_No")
			,ile."Base_Group"
			,ile."EDN_Source_Type"
			,ile."EDN_Source_No"
			,ile."EDN_Contractor_Name"
			,ile."Global_Dimension_1_Code"
			,ile."Global_Dimension_2_Code"
			,ile."Serial_No"
			,ile."Location_Code"
			,ile."Quantity"
			,ile."Invoiced_Quantity"
			,ile."Remaining_Quantity"
			,ile."Shipped_Qty_Not_Returned"
			,ile."Reserved_Quantity"
			,ile."Sales_Amount_Expected"
			,ile."Sales_Amount_Actual"
			,ile."Cost_Amount_Expected"
			,ile."Cost_Amount_Actual"
			,ile."Completely_Invoiced"
			,ile."Open"
			,ile."Order_Line_No"
			,ile."Prod_Order_Comp_Line_No"
			,ile."Dimension_Set_ID"
			,ile."Shortcut_Dimension_3_Code"
			,ile."Shortcut_Dimension_4_Code"
			,ile."Shortcut_Dimension_5_Code"
			,ile."Shortcut_Dimension_6_Code"
			,ile."Shortcut_Dimension_7_Code"
			,ile."Shortcut_Dimension_8_Code"
			,ile."EDN_Campaign_No"
			,ile."EDN_Campaign_Description"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I ile
    ON CONFLICT ("Entry_No") DO NOTHING

    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_items_ledger_entries()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_items_ledger_entries_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Document_Type"
		,"Document_No"
		,"Document_Line_No"
		,"Item_No"
		,"Key_Item_No"
		,"Base_Group"
		,"EDN_Source_Type"
		,"EDN_Source_No"
		,"EDN_Contractor_Name"
		,"Global_Dimension_1_Code"
		,"Global_Dimension_2_Code"
		,"Serial_No"
		,"Location_Code"
		,"Quantity"
		,"Invoiced_Quantity"
		,"Remaining_Quantity"
		,"Shipped_Qty_Not_Returned"
		,"Reserved_Quantity"
		,"Sales_Amount_Expected"
		,"Sales_Amount_Actual"
		,"Cost_Amount_Expected"
		,"Cost_Amount_Actual"
		,"Completely_Invoiced"
		,"Open"
		,"Order_Line_No"
		,"Prod_Order_Comp_Line_No"
		,"Dimension_Set_ID"
		,"Shortcut_Dimension_3_Code"
		,"Shortcut_Dimension_4_Code"
		,"Shortcut_Dimension_5_Code"
		,"Shortcut_Dimension_6_Code"
		,"Shortcut_Dimension_7_Code"
		,"Shortcut_Dimension_8_Code"
		,"EDN_Campaign_No"
		,"EDN_Campaign_Description"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40   -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Entry_No") DO UPDATE
	SET
		"Posting_Date" = EXCLUDED."Posting_Date"
		,"Entry_Type" = EXCLUDED."Entry_Type"
		,"Document_Type" = EXCLUDED."Document_Type"
		,"Document_No" = EXCLUDED."Document_No"
		,"Document_Line_No" = EXCLUDED."Document_Line_No"
		,"Item_No" = EXCLUDED."Item_No"
		,"Key_Item_No" = EXCLUDED."Key_Item_No"
		,"Base_Group" = EXCLUDED."Base_Group"
		,"EDN_Source_Type" = EXCLUDED."EDN_Source_Type"
		,"EDN_Source_No" = EXCLUDED."EDN_Source_No"
		,"EDN_Contractor_Name" = EXCLUDED."EDN_Contractor_Name"
		,"Global_Dimension_1_Code" = EXCLUDED."Global_Dimension_1_Code"
		,"Global_Dimension_2_Code" = EXCLUDED."Global_Dimension_2_Code"
		,"Serial_No" = EXCLUDED."Serial_No"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Quantity" = EXCLUDED."Quantity"
		,"Invoiced_Quantity" = EXCLUDED."Invoiced_Quantity"
		,"Remaining_Quantity" = EXCLUDED."Remaining_Quantity"
		,"Shipped_Qty_Not_Returned" = EXCLUDED."Shipped_Qty_Not_Returned"
		,"Reserved_Quantity" = EXCLUDED."Reserved_Quantity"
		,"Sales_Amount_Expected" = EXCLUDED."Sales_Amount_Expected"
		,"Sales_Amount_Actual" = EXCLUDED."Sales_Amount_Actual"
		,"Cost_Amount_Expected" = EXCLUDED."Cost_Amount_Expected"
		,"Cost_Amount_Actual" = EXCLUDED."Cost_Amount_Actual"
		,"Completely_Invoiced" = EXCLUDED."Completely_Invoiced"
		,"Open" = EXCLUDED."Open"
		,"Order_Line_No" = EXCLUDED."Order_Line_No"
		,"Prod_Order_Comp_Line_No" = EXCLUDED."Prod_Order_Comp_Line_No"
		,"Dimension_Set_ID" = EXCLUDED."Dimension_Set_ID"
		,"Shortcut_Dimension_3_Code" = EXCLUDED."Shortcut_Dimension_3_Code"
		,"Shortcut_Dimension_4_Code" = EXCLUDED."Shortcut_Dimension_4_Code"
		,"Shortcut_Dimension_5_Code" = EXCLUDED."Shortcut_Dimension_5_Code"
		,"Shortcut_Dimension_6_Code" = EXCLUDED."Shortcut_Dimension_6_Code"
		,"Shortcut_Dimension_7_Code" = EXCLUDED."Shortcut_Dimension_7_Code"
		,"Shortcut_Dimension_8_Code" = EXCLUDED."Shortcut_Dimension_8_Code"
		,"EDN_Campaign_No" = EXCLUDED."EDN_Campaign_No"
		,"EDN_Campaign_Description" = EXCLUDED."EDN_Campaign_Description"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
        NEW."Entry_No"
        ,NEW."Posting_Date"
        ,NEW."Entry_Type"
        ,NEW."Document_Type"
        ,NEW."Document_No"
        ,NEW."Document_Line_No"
        ,NEW."Item_No"
		,CONCAT(litera_firmy, '_', NEW."Item_No")
        ,NEW."Base_Group"
        ,NEW."EDN_Source_Type"
        ,NEW."EDN_Source_No"
		,NEW."EDN_Contractor_Name"
        ,NEW."Global_Dimension_1_Code"
        ,NEW."Global_Dimension_2_Code"
        ,NEW."Serial_No"
        ,NEW."Location_Code"
        ,NEW."Quantity"
        ,NEW."Invoiced_Quantity"
        ,NEW."Remaining_Quantity"
        ,NEW."Shipped_Qty_Not_Returned"
        ,NEW."Reserved_Quantity"
        ,NEW."Sales_Amount_Expected"
        ,NEW."Sales_Amount_Actual"
        ,NEW."Cost_Amount_Expected"
        ,NEW."Cost_Amount_Actual"
        ,NEW."Completely_Invoiced"
        ,NEW."Open"
        ,NEW."Order_Line_No"
        ,NEW."Prod_Order_Comp_Line_No"
        ,NEW."Dimension_Set_ID"
        ,NEW."Shortcut_Dimension_3_Code"
        ,NEW."Shortcut_Dimension_4_Code"
        ,NEW."Shortcut_Dimension_5_Code"
        ,NEW."Shortcut_Dimension_6_Code"
        ,NEW."Shortcut_Dimension_7_Code"
        ,NEW."Shortcut_Dimension_8_Code"
        ,NEW."EDN_Campaign_No"
        ,NEW."EDN_Campaign_Description"
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
	grupa_tabel text := 'items_ledger_entries'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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