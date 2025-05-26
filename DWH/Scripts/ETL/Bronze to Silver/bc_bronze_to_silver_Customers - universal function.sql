------------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES HEADER TABLES IN SILVER LAYER AND FIRST LOAD
------------------------------------------------------------------------------


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
	_tabela := format('bc_customers_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text PRIMARY KEY
			,"Contact_Type"	text NULL
			,"Name" text NULL
			,"Name_2" text NULL
			,"Search_Name" text NULL
			,"IC_Partner_Code" text NULL
			,"Balance_LCY" numeric(14, 2) NULL
			,"Balance_Due_LCY" numeric(14, 2) NULL
			,"Credit_Limit_LCY" numeric(14, 2) NULL
			,"EDN_Black_List" bool NULL
			,"Salesperson_Code" text NULL
			,"TotalSales2" numeric(14, 2) NULL
			,"CustSalesLCY_CustProfit_AdjmtCostLCY" numeric(14, 2) NULL
			,"AdjCustProfit" numeric(14, 2) NULL
			,"EDN_NAV_Key" text NULL
			,"VIP" bool NULL
			,"Company_Profile" text NULL
			,"Company_SegmentA" text NULL
			,"MagentoID" int4 NULL
			,"EDN_Reckoning_Limit_LCY" numeric(14, 2) NULL
			,"EDN_Used_Limit_LCY" numeric(14, 2) NULL
			,"EDN_Factoring_Reckoning" bool NULL
			,"EDN_Insurance_Customer" bool NULL
			,"EDN_Limit_Amount_Insur_LCY" numeric(14, 2) NULL
			,"Address" text NULL
			,"Country_Region_Code" text NULL
			,"City" text NULL
			,"County" text NULL
			,"Post_Code" text NULL
			,"EDN_Province_Code" text NULL
			,"VAT_Registration_No" text NULL
			,"Gen_Bus_Posting_Group" text NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Contact_Type"
			,"Name"
			,"Name_2"
			,"Search_Name"
			,"IC_Partner_Code"
			,"Balance_LCY"
			,"Balance_Due_LCY"
			,"Credit_Limit_LCY"
			,"EDN_Black_List"
			,"Salesperson_Code"
			,"TotalSales2"
			,"CustSalesLCY_CustProfit_AdjmtCostLCY"
			,"AdjCustProfit"
			,"EDN_NAV_Key"
			,"VIP"
			,"Company_Profile"
			,"Company_SegmentA"
			,"MagentoID"
			,"EDN_Reckoning_Limit_LCY"
			,"EDN_Used_Limit_LCY"
			,"EDN_Factoring_Reckoning"
			,"EDN_Insurance_Customer"
			,"EDN_Limit_Amount_Insur_LCY"
			,"Address"
			,"Country_Region_Code"
			,"City"
			,"County"
			,"Post_Code"
			,"EDN_Province_Code"
			,"VAT_Registration_No"
			,"Gen_Bus_Posting_Group"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			c."No"
			,c."Contact_Type"
			,c."Name"
			,c."Name_2"
			,c."Search_Name"
			,c."IC_Partner_Code"
			,c."Balance_LCY"
			,c."Balance_Due_LCY"
			,c."Credit_Limit_LCY"
			,c."EDN_Black_List"
			,c."Salesperson_Code"
			,c."TotalSales2"
			,c."CustSalesLCY_CustProfit_AdjmtCostLCY"
			,c."AdjCustProfit"
			,c."EDN_NAV_Key"
			,c."VIP"
			,c."Company_Profile"
			,c."Company_SegmentA"
			,c."MagentoID"
			,c."EDN_Reckoning_Limit__x005B_LCY_x005D_"
			,c."EDN_Used_Limit__x005B_LCY_x005D_"
			,c."EDN_Factoring_Reckoning"
			,c."EDN_Insurance_Customer"
			,c."EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
			,c."Address"
			,c."Country_Region_Code"
			,INITCAP(TRIM(c."City"))
			,LOWER(TRIM(c."County"))
			,c."Post_Code"
			,c."EDN_Province_Code"
			,REGEXP_REPLACE(c."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
			,c."Gen_Bus_Posting_Group"
			,c."Payment_Terms_Code"
			,c."Payment_Method_Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I c

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("No") DO UPDATE
--		SET
--			"No" = EXCLUDED."No"
--			,"Contact_Type" = EXCLUDED."Contact_Type"
--			,"Name" = EXCLUDED."Name"
--			,"Name_2" = EXCLUDED."Name_2"
--			,"Search_Name" = EXCLUDED."Search_Name"
--			,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
--			,"Balance_LCY" = EXCLUDED."Balance_LCY"
--			,"Balance_Due_LCY" = EXCLUDED."Balance_Due_LCY"
--			,"Credit_Limit_LCY" = EXCLUDED."Credit_Limit_LCY"
--			,"EDN_Black_List" = EXCLUDED."EDN_Black_List"
--			,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
--			,"TotalSales2" = EXCLUDED."TotalSales2"
--			,"CustSalesLCY_CustProfit_AdjmtCostLCY" = EXCLUDED."CustSalesLCY_CustProfit_AdjmtCostLCY"
--			,"AdjCustProfit" = EXCLUDED."AdjCustProfit"
--			,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
--			,"VIP" = EXCLUDED."VIP"
--			,"Company_Profile" = EXCLUDED."Company_Profile"
--			,"Company_SegmentA" = EXCLUDED."Company_SegmentA"
--			,"MagentoID" = EXCLUDED."MagentoID"
--			,"EDN_Reckoning_Limit_LCY" = EXCLUDED."EDN_Reckoning_Limit_LCY"
--			,"EDN_Used_Limit_LCY" = EXCLUDED."EDN_Used_Limit_LCY"
--			,"EDN_Factoring_Reckoning" = EXCLUDED."EDN_Factoring_Reckoning"
--			,"EDN_Insurance_Customer" = EXCLUDED."EDN_Insurance_Customer"
--			,"EDN_Limit_Amount_Insur_LCY" = EXCLUDED."EDN_Limit_Amount_Insur_LCY"
--			,"Address" = EXCLUDED."Address"
--			,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
--			,"City" = EXCLUDED."City"
--			,"County" = EXCLUDED."County"
--			,"Post_Code" = EXCLUDED."Post_Code"
--			,"EDN_Province_Code" = EXCLUDED."EDN_Province_Code"
--			,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
--			,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
--			,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
--			,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
--			,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_customers()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_customers_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"Contact_Type"
		,"Name"
		,"Name_2"
		,"Search_Name"
		,"IC_Partner_Code"
		,"Balance_LCY"
		,"Balance_Due_LCY"
		,"Credit_Limit_LCY"
		,"EDN_Black_List"
		,"Salesperson_Code"
		,"TotalSales2"
		,"CustSalesLCY_CustProfit_AdjmtCostLCY"
		,"AdjCustProfit"
		,"EDN_NAV_Key"
		,"VIP"
		,"Company_Profile"
		,"Company_SegmentA"
		,"MagentoID"
		,"EDN_Reckoning_Limit_LCY"
		,"EDN_Used_Limit_LCY"
		,"EDN_Factoring_Reckoning"
		,"EDN_Insurance_Customer"
		,"EDN_Limit_Amount_Insur_LCY"
		,"Address"
		,"Country_Region_Code"
		,"City"
		,"County"
		,"Post_Code"
		,"EDN_Province_Code"
		,"VAT_Registration_No"
		,"Gen_Bus_Posting_Group"
		,"Payment_Terms_Code"
		,"Payment_Method_Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"Contact_Type" = EXCLUDED."Contact_Type"
		,"Name" = EXCLUDED."Name"
		,"Name_2" = EXCLUDED."Name_2"
		,"Search_Name" = EXCLUDED."Search_Name"
		,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
		,"Balance_LCY" = EXCLUDED."Balance_LCY"
		,"Balance_Due_LCY" = EXCLUDED."Balance_Due_LCY"
		,"Credit_Limit_LCY" = EXCLUDED."Credit_Limit_LCY"
		,"EDN_Black_List" = EXCLUDED."EDN_Black_List"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"TotalSales2" = EXCLUDED."TotalSales2"
		,"CustSalesLCY_CustProfit_AdjmtCostLCY" = EXCLUDED."CustSalesLCY_CustProfit_AdjmtCostLCY"
		,"AdjCustProfit" = EXCLUDED."AdjCustProfit"
		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
		,"VIP" = EXCLUDED."VIP"
		,"Company_Profile" = EXCLUDED."Company_Profile"
		,"Company_SegmentA" = EXCLUDED."Company_SegmentA"
		,"MagentoID" = EXCLUDED."MagentoID"
		,"EDN_Reckoning_Limit_LCY" = EXCLUDED."EDN_Reckoning_Limit_LCY"
		,"EDN_Used_Limit_LCY" = EXCLUDED."EDN_Used_Limit_LCY"
		,"EDN_Factoring_Reckoning" = EXCLUDED."EDN_Factoring_Reckoning"
		,"EDN_Insurance_Customer" = EXCLUDED."EDN_Insurance_Customer"
		,"EDN_Limit_Amount_Insur_LCY" = EXCLUDED."EDN_Limit_Amount_Insur_LCY"
		,"Address" = EXCLUDED."Address"
		,"Country_Region_Code" = EXCLUDED."Country_Region_Code"
		,"City" = EXCLUDED."City"
		,"County" = EXCLUDED."County"
		,"Post_Code" = EXCLUDED."Post_Code"
		,"EDN_Province_Code" = EXCLUDED."EDN_Province_Code"
		,"VAT_Registration_No" = EXCLUDED."VAT_Registration_No"
		,"Gen_Bus_Posting_Group" = EXCLUDED."Gen_Bus_Posting_Group"
		,"Payment_Terms_Code" = EXCLUDED."Payment_Terms_Code"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,NEW."Contact_Type"
		,NEW."Name"
		,NEW."Name_2"
		,NEW."Search_Name"
		,NEW."IC_Partner_Code"
		,NEW."Balance_LCY"
		,NEW."Balance_Due_LCY"
		,NEW."Credit_Limit_LCY"
		,NEW."EDN_Black_List"
		,NEW."Salesperson_Code"
		,NEW."TotalSales2"
		,NEW."CustSalesLCY_CustProfit_AdjmtCostLCY"
		,NEW."AdjCustProfit"
		,NEW."EDN_NAV_Key"
		,NEW."VIP"
		,NEW."Company_Profile"
		,NEW."Company_SegmentA"
		,NEW."MagentoID"
		,NEW."EDN_Reckoning_Limit__x005B_LCY_x005D_"
		,NEW."EDN_Used_Limit__x005B_LCY_x005D_"
		,NEW."EDN_Factoring_Reckoning"
		,NEW."EDN_Insurance_Customer"
		,NEW."EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
		,NEW."Address"
		,NEW."Country_Region_Code"
		,INITCAP(TRIM(NEW."City"))
		,LOWER(TRIM(NEW."County"))
		,NEW."Post_Code"
		,NEW."EDN_Province_Code"
		,REGEXP_REPLACE(NEW."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g')
		,NEW."Gen_Bus_Posting_Group"
		,NEW."Payment_Terms_Code"
		,NEW."Payment_Method_Code"
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
	grupa_tabel text := 'customers'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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