-------------------------------------------------------------
-- CREATING SALES LINES TABLES IN SILVER LAYER AND FIRST LOAD
-------------------------------------------------------------



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
	_tabela := format('bc_sales_lines_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Document_Type" text NOT NULL
			,"Document_No" text NOT NULL
			,"Key_Document_No" text Null
			,"Line_No" int4 NOT NULL 
			,"Quantity" numeric(14,2) NULL
			,"Amount" numeric(14,2) NULL
			,"Amount_Including_VAT" numeric(14,2) NULL
			,"Bom_Item_No" text NULL
			,"Description" text NULL
			,"Description2" text NULL
			,"Dimension_Set_ID" int4 NULL
			,"Cooling_Capacity_KW" numeric(14,2) NULL
			,"Heating_Capacity_KW" numeric(14,2) NULL
			,"Line_Amount_Before_Disc" numeric(14,2) NULL
			,"Ory_Unit_Cost" numeric(14,2) NULL
			,"Ory_UnitCost_LCY" numeric(14,2) NULL
			,"Price_Catalogue" numeric(14,2) NULL
			,"Price_List_Currency_Code" text NULL
			,"Price_List_Exchange_Rate" numeric(14,2) NULL
			,"Profitability" text NULL
			,"Total_Cooling_CapKW" numeric(14,2) NULL
			,"Total_Gross_Weight" numeric(14,2) NULL
			,"Total_Heating_CapKW" numeric(14,2) NULL
			,"Total_Net_Weight" numeric(14,2) NULL
			,"Total_Unit_Volume" numeric(14,2) NULL
			,"Value_After_Discount_Price" numeric(14,2) NULL
			,"Value_At_List_Price" numeric(14,2) NULL
			,"Bus_Posting_Group" text NULL
			,"Prod_Posting_Group" text NULL
			,"Line_Amount" numeric(14,2) NULL
			,"Line_Discount" numeric(14,2) NULL
			,"Line_Discount_Amount" numeric(14,2) NULL 
			,"Line_Discount_Calculation" text NULL
			,"Location_Code" text NULL
			,"No_Item" text NULL
			,"Key_No_Item" text NULL
			,"Outstanding_Amount" numeric(14,2) NULL
			,"Outstanding_Amount_LCY" numeric(14,2) NULL
			,"Planned_Delivery_Date" date NULL
			,"Posting_Date" date NULL
			,"Posting_Group" text NULL
			,"Sell_To_Customer_No" text NULL
			,"Shortcut_Dimension1_Code" text NULL
			,"Shortcut_Dimension2_Code" text NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
			,PRIMARY KEY("Key_Document_No","Line_No")
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Line_No"
			,"Quantity"
			,"Amount"
			,"Amount_Including_VAT"
			,"Bom_Item_No"
			,"Description"
			,"Description2"
			,"Dimension_Set_ID"
			,"Cooling_Capacity_KW"
			,"Heating_Capacity_KW"
			,"Line_Amount_Before_Disc"
			,"Ory_Unit_Cost"
			,"Ory_UnitCost_LCY"
			,"Price_Catalogue"
			,"Price_List_Currency_Code"
			,"Price_List_Exchange_Rate"
			,"Profitability"
			,"Total_Cooling_CapKW"
			,"Total_Gross_Weight"
			,"Total_Heating_CapKW"
			,"Total_Net_Weight"
			,"Total_Unit_Volume"
			,"Value_After_Discount_Price"
			,"Value_At_List_Price"
			,"Bus_Posting_Group"
			,"Prod_Posting_Group"
			,"Line_Amount"
			,"Line_Discount"
			,"Line_Discount_Amount"
			,"Line_Discount_Calculation"
			,"Location_Code"
			,"No_Item"
			,"Key_No_Item"
			,"Outstanding_Amount"
			,"Outstanding_Amount_LCY"
			,"Planned_Delivery_Date"
			,"Posting_Date"
			,"Posting_Group"
			,"Sell_To_Customer_No"
			,"Shortcut_Dimension1_Code"
			,"Shortcut_Dimension2_Code"
			,"Firma"
			,"load_ts"
		)
		SELECT
			sl."documentType"
			,sl."documentNo"
			,CONCAT(%L, '_', sl."documentNo")
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
			,CONCAT(%L, '_', sl."no")		
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

    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

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
			"Document_Type"
			,"Document_No"
			,"Key_Document_No"
			,"Line_No"
			,"Quantity"
			,"Amount"
			,"Amount_Including_VAT"
			,"Bom_Item_No"
			,"Description"
			,"Description2"
			,"Dimension_Set_ID"
			,"Cooling_Capacity_KW"
			,"Heating_Capacity_KW"
			,"Line_Amount_Before_Disc"
			,"Ory_Unit_Cost"
			,"Ory_UnitCost_LCY"
			,"Price_Catalogue"
			,"Price_List_Currency_Code"
			,"Price_List_Exchange_Rate"
			,"Profitability"
			,"Total_Cooling_CapKW"
			,"Total_Gross_Weight"
			,"Total_Heating_CapKW"
			,"Total_Net_Weight"
			,"Total_Unit_Volume"
			,"Value_After_Discount_Price"
			,"Value_At_List_Price"
			,"Bus_Posting_Group"
			,"Prod_Posting_Group"
			,"Line_Amount"
			,"Line_Discount"
			,"Line_Discount_Amount"
			,"Line_Discount_Calculation"
			,"Location_Code"
			,"No_Item"
			,"Key_No_Item"
			,"Outstanding_Amount"
			,"Outstanding_Amount_LCY"
			,"Planned_Delivery_Date"
			,"Posting_Date"
			,"Posting_Group"
			,"Sell_To_Customer_No"
			,"Shortcut_Dimension1_Code"
			,"Shortcut_Dimension2_Code"
			,"Firma"
			,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
		$41,$42,$43,$44,$45,$46
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_Document_No","Line_No") DO UPDATE
	SET
	    ,"Document_Type" = EXCLUDED."Document_Type"
	    ,"Document_No" = EXCLUDED."Document_No"
	    ,"Key_Document_No" = EXCLUDED."Key_Document_No"
	    ,"Line_No" = EXCLUDED."Line_No"
	    ,"Quantity" = EXCLUDED."Quantity"
	    ,"Amount" = EXCLUDED."Amount"
	    ,"Amount_Including_VAT" = EXCLUDED."Amount_Including_VAT"
	    ,"Bom_Item_No" = EXCLUDED."Bom_Item_No"
	    ,"Description" = EXCLUDED."Description"
	    ,"Description2" = EXCLUDED."Description2"
	    ,"Dimension_Set_ID" = EXCLUDED."Dimension_Set_ID"
	    ,"Cooling_Capacity_KW" = EXCLUDED."Cooling_Capacity_KW"
	    ,"Heating_Capacity_KW" = EXCLUDED."Heating_Capacity_KW"
	    ,"Line_Amount_Before_Disc" = EXCLUDED."Line_Amount_Before_Disc"
	    ,"Ory_Unit_Cost" = EXCLUDED."Ory_Unit_Cost"
	    ,"Ory_UnitCost_LCY" = EXCLUDED."Ory_UnitCost_LCY"
	    ,"Price_Catalogue" = EXCLUDED."Price_Catalogue"
	    ,"Price_List_Currency_Code" = EXCLUDED."Price_List_Currency_Code"
	    ,"Price_List_Exchange_Rate" = EXCLUDED."Price_List_Exchange_Rate"
	    ,"Profitability" = EXCLUDED."Profitability"
	    ,"Total_Cooling_CapKW" = EXCLUDED."Total_Cooling_CapKW"
	    ,"Total_Gross_Weight" = EXCLUDED."Total_Gross_Weight"
	    ,"Total_Heating_CapKW" = EXCLUDED."Total_Heating_CapKW"
	    ,"Total_Net_Weight" = EXCLUDED."Total_Net_Weight"
	    ,"Total_Unit_Volume" = EXCLUDED."Total_Unit_Volume"
	    ,"Value_After_Discount_Price" = EXCLUDED."Value_After_Discount_Price"
	    ,"Value_At_List_Price" = EXCLUDED."Value_At_List_Price"
	    ,"Bus_Posting_Group" = EXCLUDED."Bus_Posting_Group"
	    ,"Prod_Posting_Group" = EXCLUDED."Prod_Posting_Group"
	    ,"Line_Amount" = EXCLUDED."Line_Amount"
	    ,"Line_Discount" = EXCLUDED."Line_Discount"
	    ,"Line_Discount_Amount" = EXCLUDED."Line_Discount_Amount"
	    ,"Line_Discount_Calculation" = EXCLUDED."Line_Discount_Calculation"
	    ,"Location_Code" = EXCLUDED."Location_Code"
	    ,"No_Item" = EXCLUDED."No_Item"
	    ,"Key_No_Item" = EXCLUDED."Key_No_Item"
	    ,"Outstanding_Amount" = EXCLUDED."Outstanding_Amount"
	    ,"Outstanding_Amount_LCY" = EXCLUDED."Outstanding_Amount_LCY"
	    ,"Planned_Delivery_Date" = EXCLUDED."Planned_Delivery_Date"
	    ,"Posting_Date" = EXCLUDED."Posting_Date"
	    ,"Posting_Group" = EXCLUDED."Posting_Group"
	    ,"Sell_To_Customer_No" = EXCLUDED."Sell_To_Customer_No"
	    ,"Shortcut_Dimension1_Code" = EXCLUDED."Shortcut_Dimension1_Code"
	    ,"Shortcut_Dimension2_Code" = EXCLUDED."Shortcut_Dimension2_Code"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."documentType"
		,NEW."documentNo"
		,CONCAT(litera_firmy, '_', NEW."documentNo")
		,NEW."lineNo"
		,NEW."quantity"
		,NEW."amount"
		,NEW."amountIncludingVAT"
		,NEW."bomItemNo"
		,NEW."description"
		,REGEXP_REPLACE(NEW."description2", E'[\\''?]', '', 'g')
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
		,CONCAT(litera_firmy, '_', NEW."no")
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