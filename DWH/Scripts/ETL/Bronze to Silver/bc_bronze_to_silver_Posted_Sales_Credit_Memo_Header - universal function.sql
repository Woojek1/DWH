---------------------------------------------------------------------------------
-- CREATING POSTED SALES CREDIT MEMO HEADER TABLES IN SILVER LAYER AND FIRST LOAD
---------------------------------------------------------------------------------


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
	_tabela := format('bc_posted_sales_credit_memo_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			 "No" text NOT NULL
			,"Key_No_Invoice" text NULL
			,"Sell_to_Customer_No" text NULL
			,"Sell_to_Customer_Name" text NULL
			,"VAT_Registration_No" text NULL
			,"Sell_to_Address" text NULL
			,"Sell_to_Address_2" text NULL
			,"Sell_to_City" text NULL
			,"Sell_to_County" text NULL
			,"Sell_to_Post_Code" text NULL
			,"Posting_Date" date NULL
			,"Document_Date" date NULL
			,"Pre_Assigned_No" text NULL
			,"External_Document_No" text NULL
			,"Salesperson_Code" text NULL
			,"Cancelled" bool NULL
			,"Corrective" bool NULL
			,"EDN_Factoring_Invoice" bool NULL
			,"Currency_Code" text NULL
			,"Currency_Factor" numeric(38,20) NULL
			,"ITI_Correction_Reason" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Location_Code" text NULL
			,"Customer_Posting_Group" text NULL
			,"Payment_Method_Code" text NULL
			,"Correction" bool NULL
			,"Shipment_Method_Code" text NULL
			,"Shipping_Agent_Code" text NULL
			,"Invoice_Type" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("No")
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
			,"Posting_Date"
			,"Document_Date"
			,"Pre_Assigned_No"
			,"External_Document_No"
			,"Salesperson_Code"
			,"Cancelled"
			,"Corrective"
			,"EDN_Factoring_Invoice"
			,"Currency_Code"
			,"Currency_Factor"
			,"ITI_Correction_Reason"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Location_Code"
			,"Customer_Posting_Group"
			,"Payment_Method_Code"
			,"Correction"
			,"Shipment_Method_Code"
			,"Shipping_Agent_Code"
			,"Invoice_Type"
			,"Firma"
			,"load_ts"
		)
		SELECT
			mh."No"
			,concat(%L, '_', mh."No")
			,mh."Sell_to_Customer_No"
			,mh."Sell_to_Customer_Name"
			,REGEXP_REPLACE(mh."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
			,mh."Sell_to_Address"
			,mh."Sell_to_Address_2"
			,INITCAP(TRIM(mh."Sell_to_City"))
			,LOWER(TRIM(mh."Sell_to_County"))
			,mh."Sell_to_Post_Code"
			,mh."Posting_Date"
			,mh."Document_Date"
			,mh."Pre_Assigned_No"
			,mh."External_Document_No"
			,mh."Salesperson_Code"
			,mh."Cancelled"
			,mh."Corrective"
			,mh."EDN_Factoring_Invoice"
			,mh."Currency_Code"
			,NULLIF(mh."Currency_Factor", '')::numeric
			,mh."ITI_Correction_Reason"
			,mh."Shortcut_Dimension_1_Code"
			,mh."Shortcut_Dimension_2_Code"
			,mh."Location_Code"
			,mh."Customer_Posting_Group"
			,mh."Payment_Method_Code"
			,mh."Correction"
			,mh."Shipment_Method_Code"
			,mh."Shipping_Agent_Code"
			,'Faktura korygująca'
			,%L
			,CURRENT_TIMESTAMP
		FROM bronze.%I mh

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("No") DO UPDATE
--		SET
--			,"Sell_to_Customer_No" = EXCLUDED."Sell_to_Customer_No"
--			,"Sell_to_Customer_Name" = EXCLUDED."Sell_to_Customer_Name"
--			,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
--			,"Sell_to_Address" = EXCLUDED."Sell_to_Address"
--			,"Sell_to_Address_2" = EXCLUDED."Sell_to_Address_2"
--			,"Sell_to_City" = EXCLUDED."Sell_to_City"
--			,"Sell_to_County" = EXCLUDED."Sell_to_County"
--			,"Sell_to_Post_Code" = EXCLUDED."Sell_to_Post_Code"
--			,"Posting_Date" = EXCLUDED."Posting_Date"
--			,"Document_Date" = EXCLUDED."Document_Date"
--			,"Pre_Assigned_No" = EXCLUDED."Pre_Assigned_No"
--			,"External_Document_No" = EXCLUDED."External_Document_No"
--			,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
--			,"Cancelled" = EXCLUDED."Cancelled"
--			,"Corrective" = EXCLUDED."Corrective"
--			,"EDN_Factoring_Invoice" = EXCLUDED."EDN_Factoring_Invoice"
--			,"Currency_Code" = EXCLUDED."Currency_Code"
--			,"ITI_Correction_Reason" = EXCLUDED."ITI_Correction_Reason"
--			,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
--			,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
--			,"Location_Code" = EXCLUDED."Location_Code"
--			,"Customer_Posting_Group" = EXCLUDED."Customer_Posting_Group"
--			,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
--			,"Correction" = EXCLUDED."Correction"
--			,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
--			,"Firma" = EXCLUDED."Firma"
--			,"Shipping_Agent_Code" = EXCLUDED."Shipping_Agent_Code"
--			,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_sales_credit_memo_header()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_sales_credit_memo_header_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

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
		,"Posting_Date"
		,"Document_Date"
		,"Pre_Assigned_No"
		,"External_Document_No"
		,"Salesperson_Code"
		,"Cancelled"
		,"Corrective"
		,"EDN_Factoring_Invoice"
		,"Currency_Code"
		,"Currency_Factor"
		,"ITI_Correction_Reason"
		,"Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code"
		,"Location_Code"
		,"Customer_Posting_Group"
		,"Payment_Method_Code"
		,"Correction"
		,"Shipment_Method_Code"
		,"Shipping_Agent_Code"
		,"Invoice_Type"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
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
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Pre_Assigned_No" = EXCLUDED."Pre_Assigned_No"
		,"External_Document_No" = EXCLUDED."External_Document_No"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Cancelled" = EXCLUDED."Cancelled"
		,"Corrective" = EXCLUDED."Corrective"
		,"EDN_Factoring_Invoice" = EXCLUDED."EDN_Factoring_Invoice"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Currency_Factor" = EXCLUDED."Currency_Factor"
		,"ITI_Correction_Reason" = EXCLUDED."ITI_Correction_Reason"
		,"Shortcut_Dimension_1_Code" = EXCLUDED."Shortcut_Dimension_1_Code"
		,"Shortcut_Dimension_2_Code" = EXCLUDED."Shortcut_Dimension_2_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Customer_Posting_Group" = EXCLUDED."Customer_Posting_Group"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Correction" = EXCLUDED."Correction"
		,"Shipment_Method_Code" = EXCLUDED."Shipment_Method_Code"
		,"Shipping_Agent_Code" = EXCLUDED."Shipping_Agent_Code"
		,"Invoice_Type" = EXCLUDED."Invoice_Type"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP
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
		,NEW."Posting_Date"
		,NEW."Document_Date"
		,NEW."Pre_Assigned_No"
		,NEW."External_Document_No"
		,NEW."Salesperson_Code"
		,NEW."Cancelled"
		,NEW."Corrective"
		,NEW."EDN_Factoring_Invoice"
		,NEW."Currency_Code"
		,NULLIF(NEW."Currency_Factor", '')::numeric
		,NEW."ITI_Correction_Reason"
		,NEW."Shortcut_Dimension_1_Code"
		,NEW."Shortcut_Dimension_2_Code"
		,NEW."Location_Code"
		,NEW."Customer_Posting_Group"
		,NEW."Payment_Method_Code"
		,NEW."Correction"
		,NEW."Shipment_Method_Code"
		,NEW."Shipping_Agent_Code"
		,'Faktura korygująca'
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
	grupa_tabel text := 'posted_sales_credit_memo_header'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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