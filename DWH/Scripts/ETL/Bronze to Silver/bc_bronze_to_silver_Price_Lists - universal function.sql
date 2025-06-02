------------------------------------------------------------------------------
-- CREATING PRICE LISTS TABLES IN SILVER LAYER AND FIRST LOAD
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
	_tabela := format('bc_price_lists_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"SystemId" uuid NOT NULL
			,"PriceListCode" text NULL
			,"LineNo" int8 NULL
			,"SourceType" text NULL
			,"SourceNo" text NULL
			,"SourceID" uuid NULL
			,"AssetType" text NULL
			,"AssetNo" text NULL
			,"AssetID" uuid NULL
			,"AssignToNo" text NULL
			,"AssignToParentNo" text NULL
			,"AmountType" text NULL
			,"Description" text NULL
			,"UnitPrice" numeric(18, 2) NULL
			,"UnitOfMeasureCode" text NULL
			,"MinimumQuantity" numeric(18, 2) NULL
			,"LineDiscount" numeric(18, 2) NULL
			,"StartingDate" date NULL
			,"EndingDate" date NULL
			,"CurrencyCode" text NULL
			,"AllowLineDisc" bool NULL
			,"AllowInvoiceDisc" bool NULL
			,"VatProdPostingGroup" text NULL
			,"Status" text NULL
			,"PriceType" text NULL
			,"ProductNo" text NULL
			,"SystemModifiedAt" timestamptz NULL
			,"SystemCreatedAt" timestamptz NULL
			,"CustomerBestPrice" numeric(18, 2) NULL
			,"CustomerBestDiscount" numeric(18, 2) NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"SystemId"
			,"PriceListCode"
			,"LineNo"
			,"SourceType"
			,"SourceNo"
			,"SourceID"
			,"AssetType"
			,"AssetNo"
			,"AssetID"
			,"AssignToNo"
			,"AssignToParentNo"
			,"AmountType"
			,"Description"
			,"UnitPrice"
			,"UnitOfMeasureCode"
			,"MinimumQuantity"
			,"LineDiscount"
			,"StartingDate"
			,"EndingDate"
			,"CurrencyCode"
			,"AllowLineDisc"
			,"AllowInvoiceDisc"
			,"VatProdPostingGroup"
			,"Status"
			,"PriceType"
			,"ProductNo"
			,"SystemModifiedAt"
			,"SystemCreatedAt"
			,"CustomerBestPrice"
			,"CustomerBestDiscount"
			,"Firma"
			,"load_ts"
		)
		SELECT
			pl."SystemId"
			,pl."priceListCode"
			,pl."lineNo"
			,pl."sourceType"
			,pl."sourceNo"
			,pl."sourceID"
			,pl."assetType"
			,pl."assetNo"
			,pl."assetID"
			,pl."assignToNo"
			,pl."assignToParentNo"
			,pl."amountType"
			,pl."description"
			,pl."unitPrice"
			,pl."unitOfMeasureCode"
			,pl."minimumQuantity"
			,pl."lineDiscount"
			,pl."startingDate"
			,pl."endingDate"
			,pl."currencyCode"
			,pl."allowLineDisc"
			,pl."allowInvoiceDisc"
			,pl."vatProdPostingGroup" 
			,pl."status"
			,pl."priceType"
			,pl."productNo"
			,pl."SystemModifiedAt"
			,pl."SystemCreatedAt"
			,pl."CustomerBestPrice" 
			,pl."CustomerBestDiscount"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I pl

    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_price_lists()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_price_lists_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"SystemId"
		,"PriceListCode"
		,"LineNo"
		,"SourceType"
		,"SourceNo"
		,"SourceID"
		,"AssetType"
		,"AssetNo"
		,"AssetID"
		,"AssignToNo"
		,"AssignToParentNo"
		,"AmountType"
		,"Description"
		,"UnitPrice"
		,"UnitOfMeasureCode"
		,"MinimumQuantity"
		,"LineDiscount"
		,"StartingDate"
		,"EndingDate"
		,"CurrencyCode"
		,"AllowLineDisc"
		,"AllowInvoiceDisc"
		,"VatProdPostingGroup"
		,"Status"
		,"PriceType"
		,"ProductNo"
		,"SystemModifiedAt"
		,"SystemCreatedAt"
		,"CustomerBestPrice"
		,"CustomerBestDiscount"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("SystemId") DO UPDATE
	SET
		"SystemId" = EXCLUDED."SystemId"
		,"PriceListCode" = EXCLUDED."PriceListCode"
		,"LineNo" = EXCLUDED."LineNo"
		,"SourceType" = EXCLUDED."SourceType"
		,"SourceNo" = EXCLUDED."SourceNo"
		,"SourceID" = EXCLUDED."SourceID"
		,"AssetType" = EXCLUDED."AssetType"
		,"AssetNo" = EXCLUDED."AssetNo"
		,"AssetID" = EXCLUDED."AssetID"
		,"AssignToNo" = EXCLUDED."AssignToNo"
		,"AssignToParentNo" = EXCLUDED."AssignToParentNo"
		,"AmountType" = EXCLUDED."AmountType"
		,"Description" = EXCLUDED."Description"
		,"UnitPrice" = EXCLUDED."UnitPrice"
		,"UnitOfMeasureCode" = EXCLUDED."UnitOfMeasureCode"
		,"MinimumQuantity" = EXCLUDED."MinimumQuantity"
		,"LineDiscount" = EXCLUDED."LineDiscount"
		,"StartingDate" = EXCLUDED."StartingDate"
		,"EndingDate" = EXCLUDED."EndingDate"
		,"CurrencyCode" = EXCLUDED."CurrencyCode"
		,"AllowLineDisc" = EXCLUDED."AllowLineDisc"
		,"AllowInvoiceDisc" = EXCLUDED."AllowInvoiceDisc"
		,"VatProdPostingGroup" = EXCLUDED."VatProdPostingGroup"
		,"Status" = EXCLUDED."Status"
		,"PriceType" = EXCLUDED."PriceType"
		,"ProductNo" = EXCLUDED."ProductNo"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"SystemCreatedAt" = EXCLUDED."SystemCreatedAt"
		,"CustomerBestPrice" = EXCLUDED."CustomerBestPrice"
		,"CustomerBestDiscount" = EXCLUDED."CustomerBestDiscount"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."SystemId"
		,NEW."priceListCode"
		,NEW."lineNo"
		,NEW."sourceType"
		,NEW."sourceNo"
		,NEW."sourceID"
		,NEW."assetType"
		,NEW."assetNo"
		,NEW."assetID"
		,NEW."assignToNo"
		,NEW."assignToParentNo"
		,NEW."amountType"
		,NEW."description"
		,NEW."unitPrice"
		,NEW."unitOfMeasureCode"
		,NEW."minimumQuantity"
		,NEW."lineDiscount"
		,NEW."startingDate"
		,NEW."endingDate"
		,NEW."currencyCode"
		,NEW."allowLineDisc"
		,NEW."allowInvoiceDisc"
		,NEW."vatProdPostingGroup" 
		,NEW."status"
		,NEW."priceType"
		,NEW."productNo"
		,NEW."SystemModifiedAt"
		,NEW."SystemCreatedAt"
		,NEW."CustomerBestPrice" 
		,NEW."CustomerBestDiscount"
		,litera_firmy
		,CURRENT_TIMESTAMP;

RETURN NEW;
END;
$function$;



-------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON PICE LISTS TABLE
-------------------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'price_lists'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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