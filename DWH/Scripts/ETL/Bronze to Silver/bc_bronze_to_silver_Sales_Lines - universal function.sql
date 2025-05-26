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
	_tabela := format('bc_sales_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"documentType" text NOT NULL
			,"documentNo" text NOT NULL
			,"lineNo" int4 NOT NULL
			,"quantity" numeric(14,2) NULL
			,"amount" numeric(14,2) NULL
			,"amountIncludingVAT" numeric(14,2) NULL
			,"bomItemNo" text NULL
			,"description" text NULL
			,"description2" text NULL
			,"dimensionSetID" int4 NULL
			,"ednCoolingCapacityKW" numeric(14,2) NULL
			,"ednHeatingCapacityKW" numeric(14,2) NULL
			,"ednLineAmountBeforeDisc" numeric(14,2) NULL
			,"ednOryUnitCost" numeric(14,2) NULL
			,"ednOryUnitCostLCY" numeric(14,2) NULL
			,"ednPriceCatalogue" numeric(14,2) NULL
			,"ednPriceListCurrencyCode" text NULL
			,"ednPriceListExchangeRate" numeric(14,2) NULL
			,"ednProfitability" text NULL
			,"ednTotalCoolingCapKW" numeric(14,2) NULL
			,"ednTotalGrossWeight" numeric(14,2) NULL
			,"ednTotalHeatingCapKW" numeric(14,2) NULL
			,"ednTotalNetWeight" numeric(14,2) NULL
			,"ednTotalUnitVolume" numeric(14,2) NULL
			,"ednValueAfterDiscountPrice" numeric(14,2) NULL
			,"ednValueAtListPrice" numeric(14,2) NULL
			,"genBusPostingGroup" text NULL
			,"genProdPostingGroup" text NULL
			,"lineAmount" numeric(14,2) NULL
			,"lineDiscount" numeric(14,2) NULL
			,"lineDiscountAmount" numeric(14,2) NULL
			,"lineDiscountCalculation" text NULL
			,"locationCode" text NULL
			,"no" text NULL
			,"outstandingAmount" numeric(14,2) NULL
			,"outstandingAmountLCY" numeric(14,2) NULL
			,"plannedDeliveryDate" date NULL
			,"postingDate" date NULL
			,"postingGroup" text NULL
			,"sellToCustomerNo" text NULL
			,"shortcutDimension1Code" text NULL
			,"shortcutDimension2Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY("documentNo","lineNo")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"documentType"
			,"documentNo"
			,"lineNo"
			,"quantity"
			,"amount"
			,"amountIncludingVAT"
			,"bomItemNo"
			,"description"
			,"description2"
			,"dimensionSetID"
			,"ednCoolingCapacityKW"
			,"ednHeatingCapacityKW"
			,"ednLineAmountBeforeDisc"
			,"ednOryUnitCost"
			,"ednOryUnitCostLCY"
			,"ednPriceCatalogue"
			,"ednPriceListCurrencyCode"
			,"ednPriceListExchangeRate"
			,"ednProfitability"
			,"ednTotalCoolingCapKW"
			,"ednTotalGrossWeight"
			,"ednTotalHeatingCapKW"
			,"ednTotalNetWeight"
			,"ednTotalUnitVolume"
			,"ednValueAfterDiscountPrice"
			,"ednValueAtListPrice"
			,"genBusPostingGroup"
			,"genProdPostingGroup"
			,"lineAmount"
			,"lineDiscount"
			,"lineDiscountAmount"
			,"lineDiscountCalculation"
			,"locationCode"
			,"no"
			,"outstandingAmount"
			,"outstandingAmountLCY"
			,"plannedDeliveryDate"
			,"postingDate"
			,"postingGroup"
			,"sellToCustomerNo"
			,"shortcutDimension1Code"
			,"shortcutDimension2Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			sl."documentType"
			,sl."documentNo"
			,sl."lineNo"
			,sl."quantity"
			,sl."amount"
			,sl."amountIncludingVAT"
			,sl."bomItemNo"
			,sl."description"
			,REGEXP_REPLACE(sl."description2", E'[\\''?]', '', 'g') AS "description2"
			,sl."dimensionSetID"
			,sl."ednCoolingCapacityKW"
			,sl."ednHeatingCapacityKW"
			,sl."ednLineAmountBeforeDisc"
			,sl."ednOryUnitCost"
			,sl."ednOryUnitCostLCY"
			,sl."ednPriceCatalogue"
			,sl."ednPriceListCurrencyCode"
			,sl."ednPriceListExchangeRate"
			,sl."ednProfitability"
			,sl."ednTotalCoolingCapKW"
			,sl."ednTotalGrossWeight"
			,sl."ednTotalHeatingCapKW"
			,sl."ednTotalNetWeight"
			,sl."ednTotalUnitVolume"
			,sl."ednValueAfterDiscountPrice"
			,sl."ednValueAtListPrice"
			,sl."genBusPostingGroup"
			,sl."genProdPostingGroup"
			,sl."lineAmount"
			,sl."lineDiscount"
			,sl."lineDiscountAmount"
			,sl."lineDiscountCalculation"
			,sl."locationCode"
			,sl."no"
			,sl."outstandingAmount"
			,sl."outstandingAmountLCY"
			,sl."plannedDeliveryDate"
			,sl."postingDate"
			,sl."postingGroup"
			,sl."sellToCustomerNo"
			,sl."shortcutDimension1Code"
			,sl."shortcutDimension2Code"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I sl

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu dodatkowej kolumny w tabeli

--		ON CONFLICT ("documentNo", "lineNo") DO UPDATE
--		SET
--		"documentType" = EXCLUDED."documentType"
--		,"documentNo" = EXCLUDED."documentNo"
--		,"lineNo" = EXCLUDED."lineNo"
--		,"quantity" = EXCLUDED."quantity"
--		,"amount" = EXCLUDED."amount"
--		,"amountIncludingVAT" = EXCLUDED."amountIncludingVAT"
--		,"bomItemNo" = EXCLUDED."bomItemNo"
--		,"description" = EXCLUDED."description"
--		,"description2" = EXCLUDED."description2"
--		,"dimensionSetID" = EXCLUDED."dimensionSetID"
--		,"ednCoolingCapacityKW" = EXCLUDED."ednCoolingCapacityKW"
--		,"ednHeatingCapacityKW" = EXCLUDED."ednHeatingCapacityKW"
--		,"ednLineAmountBeforeDisc" = EXCLUDED."ednLineAmountBeforeDisc"
--		,"ednOryUnitCost" = EXCLUDED."ednOryUnitCost"
--		,"ednOryUnitCostLCY" = EXCLUDED."ednOryUnitCostLCY"
--		,"ednPriceCatalogue" = EXCLUDED."ednPriceCatalogue"
--		,"ednPriceListCurrencyCode" = EXCLUDED."ednPriceListCurrencyCode"
--		,"ednPriceListExchangeRate" = EXCLUDED."ednPriceListExchangeRate"
--		,"ednProfitability" = EXCLUDED."ednProfitability"
--		,"ednTotalCoolingCapKW" = EXCLUDED."ednTotalCoolingCapKW"
--		,"ednTotalGrossWeight" = EXCLUDED."ednTotalGrossWeight"
--		,"ednTotalHeatingCapKW" = EXCLUDED."ednTotalHeatingCapKW"
--		,"ednTotalNetWeight" = EXCLUDED."ednTotalNetWeight"
--		,"ednTotalUnitVolume" = EXCLUDED."ednTotalUnitVolume"
--		,"ednValueAfterDiscountPrice" = EXCLUDED."ednValueAfterDiscountPrice"
--		,"ednValueAtListPrice" = EXCLUDED."ednValueAtListPrice"
--		,"genBusPostingGroup" = EXCLUDED."genBusPostingGroup"
--		,"genProdPostingGroup" = EXCLUDED."genProdPostingGroup"
--		,"lineAmount" = EXCLUDED."lineAmount"
--		,"lineDiscount" = EXCLUDED."lineDiscount"
--		,"lineDiscountAmount" = EXCLUDED."lineDiscountAmount"
--		,"lineDiscountCalculation" = EXCLUDED."lineDiscountCalculation"
--		,"locationCode" = EXCLUDED."locationCode"
--		,"no" = EXCLUDED."no"
--		,"outstandingAmount" = EXCLUDED."outstandingAmount"
--		,"outstandingAmountLCY" = EXCLUDED."outstandingAmountLCY"
--		,"plannedDeliveryDate" = EXCLUDED."plannedDeliveryDate"
--		,"postingDate" = EXCLUDED."postingDate"
--		,"postingGroup" = EXCLUDED."postingGroup"
--		,"sellToCustomerNo" = EXCLUDED."sellToCustomerNo"
--		,"shortcutDimension1Code" = EXCLUDED."shortcutDimension1Code"
--		,"shortcutDimension2Code" = EXCLUDED."shortcutDimension2Code"
--		,"Firma" = EXCLUDED."Firma"
--		,"load_ts" = EXCLUDED."load_ts";
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_sales_lines()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_sales_lines_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"documentType"
		,"documentNo"
		,"lineNo"
		,"quantity"
		,"amount"
		,"amountIncludingVAT"
		,"bomItemNo"
		,"description"
		,"description2"
		,"dimensionSetID"
		,"ednCoolingCapacityKW"
		,"ednHeatingCapacityKW"
		,"ednLineAmountBeforeDisc"
		,"ednOryUnitCost"
		,"ednOryUnitCostLCY"
		,"ednPriceCatalogue"
		,"ednPriceListCurrencyCode"
		,"ednPriceListExchangeRate"
		,"ednProfitability"
		,"ednTotalCoolingCapKW"
		,"ednTotalGrossWeight"
		,"ednTotalHeatingCapKW"
		,"ednTotalNetWeight"
		,"ednTotalUnitVolume"
		,"ednValueAfterDiscountPrice"
		,"ednValueAtListPrice"
		,"genBusPostingGroup"
		,"genProdPostingGroup"
		,"lineAmount"
		,"lineDiscount"
		,"lineDiscountAmount"
		,"lineDiscountCalculation"
		,"locationCode"
		,"no"
		,"outstandingAmount"
		,"outstandingAmountLCY"
		,"plannedDeliveryDate"
		,"postingDate"
		,"postingGroup"
		,"sellToCustomerNo"
		,"shortcutDimension1Code"
		,"shortcutDimension2Code"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43, $44
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("documentNo", "lineNo") DO UPDATE
	SET
		"documentType" = EXCLUDED."documentType"
		,"documentNo" = EXCLUDED."documentNo"
		,"lineNo" = EXCLUDED."lineNo"
		,"quantity" = EXCLUDED."quantity"
		,"amount" = EXCLUDED."amount"
		,"amountIncludingVAT" = EXCLUDED."amountIncludingVAT"
		,"bomItemNo" = EXCLUDED."bomItemNo"
		,"description" = EXCLUDED."description"
		,"description2" = EXCLUDED."description2"
		,"dimensionSetID" = EXCLUDED."dimensionSetID"
		,"ednCoolingCapacityKW" = EXCLUDED."ednCoolingCapacityKW"
		,"ednHeatingCapacityKW" = EXCLUDED."ednHeatingCapacityKW"
		,"ednLineAmountBeforeDisc" = EXCLUDED."ednLineAmountBeforeDisc"
		,"ednOryUnitCost" = EXCLUDED."ednOryUnitCost"
		,"ednOryUnitCostLCY" = EXCLUDED."ednOryUnitCostLCY"
		,"ednPriceCatalogue" = EXCLUDED."ednPriceCatalogue"
		,"ednPriceListCurrencyCode" = EXCLUDED."ednPriceListCurrencyCode"
		,"ednPriceListExchangeRate" = EXCLUDED."ednPriceListExchangeRate"
		,"ednProfitability" = EXCLUDED."ednProfitability"
		,"ednTotalCoolingCapKW" = EXCLUDED."ednTotalCoolingCapKW"
		,"ednTotalGrossWeight" = EXCLUDED."ednTotalGrossWeight"
		,"ednTotalHeatingCapKW" = EXCLUDED."ednTotalHeatingCapKW"
		,"ednTotalNetWeight" = EXCLUDED."ednTotalNetWeight"
		,"ednTotalUnitVolume" = EXCLUDED."ednTotalUnitVolume"
		,"ednValueAfterDiscountPrice" = EXCLUDED."ednValueAfterDiscountPrice"
		,"ednValueAtListPrice" = EXCLUDED."ednValueAtListPrice"
		,"genBusPostingGroup" = EXCLUDED."genBusPostingGroup"
		,"genProdPostingGroup" = EXCLUDED."genProdPostingGroup"
		,"lineAmount" = EXCLUDED."lineAmount"
		,"lineDiscount" = EXCLUDED."lineDiscount"
		,"lineDiscountAmount" = EXCLUDED."lineDiscountAmount"
		,"lineDiscountCalculation" = EXCLUDED."lineDiscountCalculation"
		,"locationCode" = EXCLUDED."locationCode"
		,"no" = EXCLUDED."no"
		,"outstandingAmount" = EXCLUDED."outstandingAmount"
		,"outstandingAmountLCY" = EXCLUDED."outstandingAmountLCY"
		,"plannedDeliveryDate" = EXCLUDED."plannedDeliveryDate"
		,"postingDate" = EXCLUDED."postingDate"
		,"postingGroup" = EXCLUDED."postingGroup"
		,"sellToCustomerNo" = EXCLUDED."sellToCustomerNo"
		,"shortcutDimension1Code" = EXCLUDED."shortcutDimension1Code"
		,"shortcutDimension2Code" = EXCLUDED."shortcutDimension2Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."documentType"
		,NEW."documentNo"
		,NEW."lineNo"
		,NEW."quantity"
		,NEW."amount"
		,NEW."amountIncludingVAT"
		,NEW."bomItemNo"
		,NEW."description"
		,REGEXP_REPLACE(NEW."description2", E'[\\''?]', '', 'g') AS "description2"
		,NEW."dimensionSetID"
		,NEW."ednCoolingCapacityKW"
		,NEW."ednHeatingCapacityKW"
		,NEW."ednLineAmountBeforeDisc"
		,NEW."ednOryUnitCost"
		,NEW."ednOryUnitCostLCY"
		,NEW."ednPriceCatalogue"
		,NEW."ednPriceListCurrencyCode"
		,NEW."ednPriceListExchangeRate"
		,NEW."ednProfitability"
		,NEW."ednTotalCoolingCapKW"
		,NEW."ednTotalGrossWeight"
		,NEW."ednTotalHeatingCapKW"
		,NEW."ednTotalNetWeight"
		,NEW."ednTotalUnitVolume"
		,NEW."ednValueAfterDiscountPrice"
		,NEW."ednValueAtListPrice"
		,NEW."genBusPostingGroup"
		,NEW."genProdPostingGroup"
		,NEW."lineAmount"
		,NEW."lineDiscount"
		,NEW."lineDiscountAmount"
		,NEW."lineDiscountCalculation"
		,NEW."locationCode"
		,NEW."no"
		,NEW."outstandingAmount"
		,NEW."outstandingAmountLCY"
		,NEW."plannedDeliveryDate"
		,NEW."postingDate"
		,NEW."postingGroup"
		,NEW."sellToCustomerNo"
		,NEW."shortcutDimension1Code"
		,NEW."shortcutDimension2Code"
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
	grupa_tabel text := 'sales_lines'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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