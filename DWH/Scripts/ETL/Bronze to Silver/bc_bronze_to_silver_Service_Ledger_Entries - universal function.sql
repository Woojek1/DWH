----------------------------------------------------------------------
-- CREATING ITEMS LEDGER ENTRIES TABLES IN SILVER LAYER AND FIRST LOAD
----------------------------------------------------------------------


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
	_tabela := format('bc_service_ledger_entry_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---

-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Entry_No" int4 NOT NULL PRIMARY KEY
			,"Posting_Date" date NULL
			,"Entry_Type" text NULL
			,"Service_Order_No" text NULL
			,"Job_No" text NULL
			,"Document_No" text NULL
			,"Bill_to_Customer_No" text NULL
			,"Service_Item_No_Serviced" text NULL
			,"Item_No_Serviced" text NULL
			,"Base_Group_A_Serviced" text NULL
			,"Base_Group_B_Serviced" text NULL
			,"Serial_No_Serviced" text NULL
			,"Contract_Invoice_Period" text NULL
			,"Global_Dimension_1_Code" text NULL
			,"Global_Dimension_2_Code" text NULL
			,"Contract_Group_Code" text NULL
			,"Type" text NULL
			,"No" text NULL
			,"Base_GroupA" text NULL
			,"Base_GroupB" text NULL
			,"Cost_Amount" numeric(18, 5) NULL
			,"Discount_Amount" numeric(18, 5) NULL
			,"Unit_Cost" numeric(18, 5) NULL
			,"Quantity" numeric(18, 5) NULL
			,"Charged_Qty" numeric(18, 5) NULL
			,"Unit_Price" numeric(18, 5) NULL
			,"Description" text NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"Dimension_Set_ID" int4 NULL
			,"Shortcut_Dimension_8_Code" text NULL
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
			,"Service_Order_No"
			,"Job_No"
			,"Document_No"
			,"Bill_to_Customer_No"
			,"Service_Item_No_Serviced"
			,"Item_No_Serviced"
			,"Base_Group_A_Serviced"
			,"Base_Group_B_Serviced"
			,"Serial_No_Serviced"
			,"Contract_Invoice_Period"
			,"Global_Dimension_1_Code" 
			,"Global_Dimension_2_Code"
			,"Contract_Group_Code"
			,"Type"
			,"No"
			,"Base_GroupA"
			,"Base_GroupB"
			,"Cost_Amount"
			,"Discount_Amount"
			,"Unit_Cost"
			,"Quantity"
			,"Charged_Qty"
			,"Unit_Price"
			,"Description"
			,"Gen_Bus_Posting_Group"
			,"Gen_Prod_Posting_Group"
			,"Dimension_Set_ID"
			,"Shortcut_Dimension_8_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			sle."Entry_No"
			,sle."Posting_Date"
			,sle."Entry_Type"
			,sle."Service_Order_No"
			,sle."Job_No"
			,sle."Document_No"
			,sle."Bill_to_Customer_No"
			,sle."Service_Item_No_Serviced"
			,sle."Item_No_Serviced"
			,sle."Base_Group_A_Serviced"
			,sle."Base_Group_B_Serviced"
			,sle."Serial_No_Serviced"
			,sle."Contract_Invoice_Period"
			,sle."Global_Dimension_1_Code" 
			,sle."Global_Dimension_2_Code"
			,sle."Contract_Group_Code"
			,sle."Type"
			,sle."No"
			,sle."Base_GroupA"
			,sle."Base_GroupB"
			,sle."Cost_Amount"
			,sle."Discount_Amount"
			,sle."Unit_Cost"
			,sle."Quantity"
			,sle."Charged_Qty"
			,sle."Unit_Price"
			,sle."Description"
			,sle."Gen_Bus_Posting_Group"
			,sle."Gen_Prod_Posting_Group"
			,sle."Dimension_Set_ID"
			,sle."Shortcut_Dimension_8_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I sle

    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_service_ledger_entry()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_items_ledger_entry_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Entry_No"
		,"Posting_Date"
		,"Entry_Type"
		,"Service_Order_No"
		,"Job_No"
		,"Document_No"
		,"Bill_to_Customer_No"
		,"Service_Item_No_Serviced"
		,"Item_No_Serviced"
		,"Base_Group_A_Serviced"
		,"Base_Group_B_Serviced"
		,"Serial_No_Serviced"
		,"Contract_Invoice_Period"
		,"Global_Dimension_1_Code" 
		,"Global_Dimension_2_Code"
		,"Contract_Group_Code"
		,"Type"
		,"No"
		,"Base_GroupA"
		,"Base_GroupB"
		,"Cost_Amount"
		,"Discount_Amount"
		,"Unit_Cost"
		,"Quantity"
		,"Charged_Qty"
		,"Unit_Price"
		,"Description"
		,"Gen_Bus_Posting_Group"
		,"Gen_Prod_Posting_Group"
		,"Dimension_Set_ID"
		,"Shortcut_Dimension_8_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Entry_No") DO UPDATE
	SET
		"Entry_No" = EXCLUDED."Entry_No"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Entry_Type" = EXCLUDED."Entry_Type"
		,"Service_Order_No" = EXCLUDED."Service_Order_No"
		,"Job_No" = EXCLUDED."Job_No"
		,"Document_No" = EXCLUDED."Document_No"
		,"Bill_to_Customer_No" = EXCLUDED."Bill_to_Customer_No"
		,"Service_Item_No_Serviced" = EXCLUDED."Service_Item_No_Serviced"
		,"Item_No_Serviced" = EXCLUDED."Item_No_Serviced"
		,"Base_Group_A_Serviced" = EXCLUDED."Base_Group_A_Serviced"
		,"Base_Group_B_Serviced" = EXCLUDED."Base_Group_B_Serviced"
		,"Serial_No_Serviced" = EXCLUDED."Serial_No_Serviced"
		,"Contract_Invoice_Period" = EXCLUDED."Contract_Invoice_Period"
		,"Global_Dimension_1_Code"  = EXCLUDED."Global_Dimension_1_Code"
		,"Global_Dimension_2_Code" = EXCLUDED."Global_Dimension_2_Code"
		,"Contract_Group_Code" = EXCLUDED."Contract_Group_Code"
		,"Type" = EXCLUDED."Type"
		,"No" = EXCLUDED."No"
		,"Base_GroupA" = EXCLUDED."Base_GroupA"
		,"Base_GroupB"= EXCLUDED."Base_GroupB"
		,"Cost_Amount" = EXCLUDED."Cost_Amount"
		,"Discount_Amount" = EXCLUDED."Discount_Amount"
		,"Unit_Cost" = EXCLUDED."Unit_Cost"
		,"Quantity" = EXCLUDED."Quantity"
		,"Charged_Qty" = EXCLUDED."Charged_Qty"
		,"Unit_Price" = EXCLUDED."Unit_Price"
		,"Description" = EXCLUDED."Description"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Gen_Prod_Posting_Group" = EXCLUDED."Gen_Prod_Posting_Group"
		,"Dimension_Set_ID" = EXCLUDED."Dimension_Set_ID"
		,"Shortcut_Dimension_8_Code" = EXCLUDED."Shortcut_Dimension_8_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
 		NEW."Entry_No"
		,NEW."Posting_Date"
		,NEW."Entry_Type"
		,NEW."Service_Order_No"
		,NEW."Job_No"
		,NEW."Document_No"
		,NEW."Bill_to_Customer_No"
		,NEW."Service_Item_No_Serviced"
		,NEW."Item_No_Serviced"
		,NEW."Base_Group_A_Serviced"
		,NEW."Base_Group_B_Serviced"
		,NEW."Serial_No_Serviced"
		,NEW."Contract_Invoice_Period"
		,NEW."Global_Dimension_1_Code" 
		,NEW."Global_Dimension_2_Code"
		,NEW."Contract_Group_Code"
		,NEW."Type"
		,NEW."No"
		,NEW."Base_GroupA"
		,NEW."Base_GroupB"
		,NEW."Cost_Amount"
		,NEW."Discount_Amount"
		,NEW."Unit_Cost"
		,NEW."Quantity"
		,NEW."Charged_Qty"
		,NEW."Unit_Price"
		,NEW."Description"
		,NEW."Gen_Bus_Posting_Group"
		,NEW."Gen_Prod_Posting_Group"
		,NEW."Dimension_Set_ID"
		,NEW."Shortcut_Dimension_8_Code"
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
	grupa_tabel text := 'service_ledger_entry'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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
