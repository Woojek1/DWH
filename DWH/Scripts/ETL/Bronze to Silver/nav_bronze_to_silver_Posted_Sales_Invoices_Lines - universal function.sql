-----------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES LINES TABLES IN SILVER LAYER AND FIRST LOAD
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
	_tabela := format('nav_posted_sales_invoices_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---

 
-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"DocumentNo" text NOT NULL
			,"LineNo" int4 NOT NULL
			,"Amount" numeric(14,2) NULL
			,"AmountIncludingVAT" numeric(14,2) NULL
			,"Description" text NULL
			,"Description2" text NULL
--			,"DimensionSetID" int4 NULL
--			,"EdnCampaignNo" text NULL
--			,"EdnOryUnitCost" numeric(14,5) NULL
--			,"EdnOryUnitCostLCY" numeric(14,5) NULL
--			,"EdnSalesMargin" numeric(14,4) NULL
			,"GenBusPostingGroup" text NULL
			,"GenProdPostingGroup" text NULL
			,"LineAmount" numeric(14,2) NULL
			,"LineDiscount" numeric(8,2) NULL
			,"LineDiscountAmount" numeric(14,2) NULL
			,"LocationCode" text NULL
			,"No" text NULL
			,"PostingDate" date NULL
			,"PostingGroup" text NULL
			,"Quantity" numeric(14,2) NULL
			,"SellToCustomerNo" text NULL
			,"ShortcutDimension1Code" text NULL
			,"ShortcutDimension2Code" text NULL
			,"Type" text NULL
			,"UnitCost" numeric(14,5) NULL
			,"UnitCostLCY" numeric(14,5) NULL
			,"UnitPrice" numeric(14,2) NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY ("DocumentNo", "LineNo")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"DocumentNo"
			,"LineNo"
			,"Amount"
			,"AmountIncludingVAT"
			,"Description"
			,"Description2"
--			,"DimensionSetID"
--			,"EdnCampaignNo"
--			,"EdnOryUnitCost"
--			,"EdnOryUnitCostLCY"
--			,"EdnSalesMargin"
			,"GenBusPostingGroup"
			,"GenProdPostingGroup"
			,"LineAmount"
			,"LineDiscount"
			,"LineDiscountAmount"
			,"LocationCode"
			,"No"
			,"PostingDate"
			,"PostingGroup"
			,"Quantity"
			,"SellToCustomerNo"
			,"ShortcutDimension1Code"
			,"ShortcutDimension2Code"
			,"Type"
			,"UnitCost"
			,"UnitCostLCY"
			,"UnitPrice"
			,"Firma"
			,"load_ts"
		)
		SELECT
			s."Document No_"
			,s."Line No_"
			,round(s."Amount", 2)
			,round(s."Amount Including VAT", 2)
			,s."Description"
			,s."Description 2"
--			,s."dimensionSetID"
--			,s."ednCampaignNo"
--			,s."ednOryUnitCost"
--			,s."ednOryUnitCostLCY"
--			,s."ednSalesMargin"
			,s."Gen_ Bus_ Posting Group"
			,s."Gen_ Prod_ Posting Group"
			,round(s."Line Amount", 2)
			,round(s."Line Discount %%", 2)::numeric(8,2)
			,round(s."Line Discount Amount", 2)
			,s."Location Code"
			,s."No_"
			,s."Posting Date"
			,s."Posting Group"
			,round(s."Quantity", 2)
			,s."Sell-to Customer No_"
			,s."Shortcut Dimension 1 Code"
			,s."Shortcut Dimension 2 Code"
			,s."Type"
			,round(s."Unit Cost", 5)
			,round(s."Unit Cost (LCY)", 5)
			,round(s."Unit Price", 2)
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I s
 
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_posted_sales_invoices_lines()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_posted_sales_invoices_lines_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"documentNo"
		,"lineNo"
		,amount
		,"amountIncludingVAT"
		,description
		,description2
		,"dimensionSetID"
		,"ednCampaignNo"
		,"ednOryUnitCost"
		,"ednOryUnitCostLCY"
		,"ednSalesMargin"
		,"genBusPostingGroup"
		,"genProdPostingGroup"
		,"lineAmount"
		,"lineDiscount"
		,"lineDiscountAmount"
		,"locationCode"
		,"no"
		,"postingDate"
		,"postingGroup"
		,quantity
		,"sellToCustomerNo"
		,"shortcutDimension1Code"
		,"shortcutDimension2Code"
		,"type"
		,"unitCost"
		,"unitCostLCY"
		,"unitPrice"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("documentNo", "lineNo") DO UPDATE
	SET
		"documentNo" = EXCLUDED."documentNo"
		,"lineNo" = EXCLUDED."lineNo"
		,amount = EXCLUDED.amount
		,"amountIncludingVAT" = EXCLUDED."amountIncludingVAT"
		,description = EXCLUDED.description
		,description2 = EXCLUDED.description2
		,"dimensionSetID" = EXCLUDED."dimensionSetID"
		,"ednCampaignNo" = EXCLUDED."ednCampaignNo"
		,"ednOryUnitCost" = EXCLUDED."ednOryUnitCost"
		,"ednOryUnitCostLCY" = EXCLUDED."ednOryUnitCostLCY"
		,"ednSalesMargin" = EXCLUDED."ednSalesMargin"
		,"genBusPostingGroup" = EXCLUDED."genBusPostingGroup"
		,"genProdPostingGroup" = EXCLUDED."genProdPostingGroup"
		,"lineAmount" = EXCLUDED."lineAmount"
		,"lineDiscount" = EXCLUDED."lineDiscount"
		,"lineDiscountAmount" = EXCLUDED."lineDiscountAmount"
		,"locationCode" = EXCLUDED."locationCode"
		,"no" = EXCLUDED."no"
		,"postingDate" = EXCLUDED."postingDate"
		,"postingGroup" = EXCLUDED."postingGroup"
		,quantity = EXCLUDED.quantity
		,"sellToCustomerNo" = EXCLUDED."sellToCustomerNo"
		,"shortcutDimension1Code" = EXCLUDED."shortcutDimension1Code"
		,"shortcutDimension2Code" = EXCLUDED."shortcutDimension2Code"
		,"type" = EXCLUDED."type"
		,"unitCost" = EXCLUDED."unitCost"
		,"unitCostLCY" = EXCLUDED."unitCostLCY"
		,"unitPrice" = EXCLUDED."unitPrice"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."documentNo"
		,NEW."lineNo"
		,NEW.amount
		,NEW."amountIncludingVAT"
		,NEW.description
		,NEW.description2
		,NEW."dimensionSetID"
		,NEW."ednCampaignNo"
		,NEW."ednOryUnitCost"
		,NEW."ednOryUnitCostLCY"
		,NEW."ednSalesMargin"
		,NEW."genBusPostingGroup"
		,NEW."genProdPostingGroup"
		,NEW."lineAmount"
		,NEW."lineDiscount"
		,NEW."lineDiscountAmount"
		,NEW."locationCode"
		,NEW."no"
		,NEW."postingDate"
		,NEW."postingGroup"
		,NEW."quantity"
		,NEW."sellToCustomerNo"
		,NEW."shortcutDimension1Code"
		,NEW."shortcutDimension2Code"
		,NEW."type"
		,NEW."unitCost"
		,NEW."unitCostLCY"
		,NEW."unitPrice"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON POSTED SALES INVOICES LINES TABLE
------------------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'posted_sales_invoices_lines'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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