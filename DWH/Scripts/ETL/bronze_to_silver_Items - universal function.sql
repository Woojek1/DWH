-----------------------------------------------------------
-- CREATING ITEMS TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------


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
	_tabela := format('bc_items_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text PRIMARY KEY
			,"PIM_number" text NULL
			,"Description" text NULL
			,"EDN_Description_2" text NULL
			,"Type" text NULL
			,"Base_Unit_of_Measure" text NULL
			,"EDN_Weight_Packaging" numeric(14, 4) NULL
			,"Last_Date_Modified" date NULL
			,"GTIN" text NULL
			,"Item_Category_Code" text NULL
			,"Manufacturer_Code" text NULL
			,"EDN_Brand_Code" text NULL
			,"Service_Item_Group" text NULL
			,"EDN_NAV_Key" text NULL
			,"Inventory" numeric(14, 4) NULL
			,"Qty_on_Purch_Order" numeric(14, 4) NULL
			,"Qty_on_Prod_Order" numeric(14, 4) NULL
			,"Qty_on_Component_Lines" numeric(14, 4) NULL
			,"Qty_on_Sales_Order" numeric(14, 4) NULL
			,"Qty_on_Service_Order" numeric(14, 4) NULL
			,"Qty_on_Job_Order" numeric(14, 4) NULL
			,"Qty_on_Assembly_Order" numeric(14, 4) NULL
			,"Qty_on_Asm_Component" numeric(14, 4) NULL
			,"Reserved_Qty_on_Inventory" numeric(14, 4) NULL
			,"Reserved_Qty_on_Purch_Orders" numeric(14, 4) NULL
			,"EDN_Cooling_Capacity_kW" numeric(14, 4) NULL
			,"EDN_Heating_Capacity_kW" numeric(14, 4) NULL
			,"EDN_Refrigerant" text NULL
			,"EDN_Refrigerant_Quantity" numeric(14, 4) NULL
			,"EDN_Factor_Equivalent" numeric(14, 4) NULL
			,"Costing_Method" text NULL
			,"Standard_Cost" numeric(14, 4) NULL
			,"Unit_Cost" numeric(14, 4) NULL
			,"Last_Direct_Cost" numeric(14, 4) NULL
			,"Gen_Prod_Posting_Group" text NULL
			,"Inventory_Posting_Group" text NULL
			,"B2BSales" bool NULL
			,"ecom" bool NULL
			,"Vendor_No" text NULL
			,"Safety_Stock_Quantity" numeric(14, 4) NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);



-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"PIM_number"
			,"Description"
			,"EDN_Description_2"
			,"Type"
			,"Base_Unit_of_Measure"
			,"EDN_Weight_Packaging"
			,"Last_Date_Modified"
			,"GTIN"
			,"Item_Category_Code"
			,"Manufacturer_Code"
			,"EDN_Brand_Code"
			,"Service_Item_Group"
			,"EDN_NAV_Key"
			,"Inventory"
			,"Qty_on_Purch_Order"
			,"Qty_on_Prod_Order"
			,"Qty_on_Component_Lines"
			,"Qty_on_Sales_Order"
			,"Qty_on_Service_Order"
			,"Qty_on_Job_Order"
			,"Qty_on_Assembly_Order"
			,"Qty_on_Asm_Component"
			,"Reserved_Qty_on_Inventory"
			,"Reserved_Qty_on_Purch_Orders"
			,"EDN_Cooling_Capacity_kW"
			,"EDN_Heating_Capacity_kW"
			,"EDN_Refrigerant"
			,"EDN_Refrigerant_Quantity"
			,"EDN_Factor_Equivalent"
			,"Costing_Method"
			,"Standard_Cost"
			,"Unit_Cost"
			,"Last_Direct_Cost"
			,"Gen_Prod_Posting_Group"
			,"Inventory_Posting_Group"
			,"B2BSales"
			,"ecom"
			,"Vendor_No"
			,"Safety_Stock_Quantity"
			,"Firma"
			,"load_ts"
		)
		SELECT
			i."No"
			,i."PIM_number"
			,i."Description"
			,i."EDN_Description_2"
			,i."Type"
			,i."Base_Unit_of_Measure"
			,i."EDN_Weight_Packaging"
			,i."Last_Date_Modified"
			,i."GTIN"
			,i."Item_Category_Code"
			,i."Manufacturer_Code"
			,i."EDN_Brand_Code"
			,i."Service_Item_Group"
			,i."EDN_NAV_Key"
			,i."Inventory"
			,i."Qty_on_Purch_Order"
			,i."Qty_on_Prod_Order"
			,i."Qty_on_Component_Lines"
			,i."Qty_on_Sales_Order"
			,i."Qty_on_Service_Order"
			,i."Qty_on_Job_Order"
			,i."Qty_on_Assembly_Order"
			,i."Qty_on_Asm_Component"
			,i."Reserved_Qty_on_Inventory"
			,i."Reserved_Qty_on_Purch_Orders"
			,i."EDN_Cooling_Capacity_kW"
			,i."EDN_Heating_Capacity_kW"
			,i."EDN_Refrigerant"
			,i."EDN_Refrigerant_Quantity__x005B_UoM_x005D_"
    		,i."EDN_Factor_Equivalent__x005B_UoM_x005D_"
			,i."Costing_Method"
			,i."Standard_Cost"
			,i."Unit_Cost"
			,i."Last_Direct_Cost"
			,i."Gen_Prod_Posting_Group"
			,i."Inventory_Posting_Group"
			,i."B2BSales"
			,i."ecom"
			,i."Vendor_No"
			,i."Safety_Stock_Quantity"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I as i

--	ON CONFLICT zostaje dla przeładowania danych po dodaniu doaatkowej kolumny w tabeli

--		ON CONFLICT ("No") DO UPDATE
--		SET
--			"PIM_number" = EXCLUDED."PIM_number"
--			,"Description" = EXCLUDED."Description"
--			,"EDN_Description_2" = EXCLUDED."EDN_Description_2"
--			,"Type" = EXCLUDED."Type"
--			,"Base_Unit_of_Measure" = EXCLUDED."Base_Unit_of_Measure"
--			,"EDN_Weight_Packaging" = EXCLUDED."EDN_Weight_Packaging"
--			,"Last_Date_Modified" = EXCLUDED."Last_Date_Modified"
--			,"GTIN" = EXCLUDED."GTIN"
--			,"Item_Category_Code" = EXCLUDED."Item_Category_Code"
--			,"Manufacturer_Code" = EXCLUDED."Manufacturer_Code"
--			,"EDN_Brand_Code" = EXCLUDED."EDN_Brand_Code"
--			,"Service_Item_Group" = EXCLUDED."Service_Item_Group"
--			,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
--			,"Inventory" = EXCLUDED."Inventory"
--			,"Qty_on_Purch_Order" = EXCLUDED."Qty_on_Purch_Order"
--			,"Qty_on_Prod_Order" = EXCLUDED."Qty_on_Prod_Order"
--			,"Qty_on_Component_Lines" = EXCLUDED."Qty_on_Component_Lines"
--			,"Qty_on_Sales_Order" = EXCLUDED."Qty_on_Sales_Order"
--			,"Qty_on_Service_Order" = EXCLUDED."Qty_on_Service_Order"
--			,"Qty_on_Job_Order" = EXCLUDED."Qty_on_Job_Order"
--			,"Qty_on_Assembly_Order" = EXCLUDED."Qty_on_Assembly_Order"
--			,"Qty_on_Asm_Component" = EXCLUDED."Qty_on_Asm_Component"
--			,"Reserved_Qty_on_Inventory" = EXCLUDED."Reserved_Qty_on_Inventory"
--			,"Reserved_Qty_on_Purch_Orders" = EXCLUDED."Reserved_Qty_on_Purch_Orders"
--			,"EDN_Cooling_Capacity_kW" = EXCLUDED."EDN_Cooling_Capacity_kW"
--			,"EDN_Heating_Capacity_kW" = EXCLUDED."EDN_Heating_Capacity_kW"
--			,"EDN_Refrigerant" = EXCLUDED."EDN_Refrigerant"
--			,"EDN_Refrigerant_Quantity" = EXCLUDED."EDN_Refrigerant_Quantity"
--			,"EDN_Factor_Equivalent" = EXCLUDED."EDN_Factor_Equivalent"
--			,"Costing_Method" = EXCLUDED."Costing_Method"
--			,"Standard_Cost" = EXCLUDED."Standard_Cost"
--			,"Unit_Cost" = EXCLUDED."Unit_Cost"
--			,"Last_Direct_Cost" = EXCLUDED."Last_Direct_Cost"
--			,"Gen_Prod_Posting_Group" = EXCLUDED."Gen_Prod_Posting_Group"
--			,"Inventory_Posting_Group" = EXCLUDED."Inventory_Posting_Group"
--			,"B2BSales" = EXCLUDED."B2BSales"
--			,"ecom" = EXCLUDED."ecom"
--			,"Vendor_No" = EXCLUDED."Vendor_No"
--			,"Safety_Stock_Quantity" = EXCLUDED."Safety_Stock_Quantity"
--			,"Firma" = EXCLUDED."Firma"
--			,"load_ts" = CURRENT_TIMESTAMP
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_items()  -- ZMIENIĆ NAZWĘ FUNKCJI
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
	target_table := format('bc_items_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"No"
		,"PIM_number"
		,"Description"
		,"EDN_Description_2"
		,"Type"
		,"Base_Unit_of_Measure"
		,"EDN_Weight_Packaging"
		,"Last_Date_Modified"
		,"GTIN"
		,"Item_Category_Code"
		,"Manufacturer_Code"
		,"EDN_Brand_Code"
		,"Service_Item_Group"
		,"EDN_NAV_Key"
		,"Inventory"
		,"Qty_on_Purch_Order"
		,"Qty_on_Prod_Order"
		,"Qty_on_Component_Lines"
		,"Qty_on_Sales_Order"
		,"Qty_on_Service_Order"
		,"Qty_on_Job_Order"
		,"Qty_on_Assembly_Order"
		,"Qty_on_Asm_Component"
		,"Reserved_Qty_on_Inventory"
		,"Reserved_Qty_on_Purch_Orders"
		,"EDN_Cooling_Capacity_kW"
		,"EDN_Heating_Capacity_kW"
		,"EDN_Refrigerant"
		,"EDN_Refrigerant_Quantity"
		,"EDN_Factor_Equivalent"
		,"Costing_Method"
		,"Standard_Cost"
		,"Unit_Cost"
		,"Last_Direct_Cost"
		,"Gen_Prod_Posting_Group"
		,"Inventory_Posting_Group"
		,"B2BSales"
		,"ecom"
		,"Vendor_No"
		,"Safety_Stock_Quantity"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("No") DO UPDATE
	SET
		"PIM_number" = EXCLUDED."PIM_number"
		,"Description" = EXCLUDED."Description"
		,"EDN_Description_2" = EXCLUDED."EDN_Description_2"
		,"Type" = EXCLUDED."Type"
		,"Base_Unit_of_Measure" = EXCLUDED."Base_Unit_of_Measure"
		,"EDN_Weight_Packaging" = EXCLUDED."EDN_Weight_Packaging"
		,"Last_Date_Modified" = EXCLUDED."Last_Date_Modified"
		,"GTIN" = EXCLUDED."GTIN"
		,"Item_Category_Code" = EXCLUDED."Item_Category_Code"
		,"Manufacturer_Code" = EXCLUDED."Manufacturer_Code"
		,"EDN_Brand_Code" = EXCLUDED."EDN_Brand_Code"
		,"Service_Item_Group" = EXCLUDED."Service_Item_Group"
		,"EDN_NAV_Key" = EXCLUDED."EDN_NAV_Key"
		,"Inventory" = EXCLUDED."Inventory"
		,"Qty_on_Purch_Order" = EXCLUDED."Qty_on_Purch_Order"
		,"Qty_on_Prod_Order" = EXCLUDED."Qty_on_Prod_Order"
		,"Qty_on_Component_Lines" = EXCLUDED."Qty_on_Component_Lines"
		,"Qty_on_Sales_Order" = EXCLUDED."Qty_on_Sales_Order"
		,"Qty_on_Service_Order" = EXCLUDED."Qty_on_Service_Order"
		,"Qty_on_Job_Order" = EXCLUDED."Qty_on_Job_Order"
		,"Qty_on_Assembly_Order" = EXCLUDED."Qty_on_Assembly_Order"
		,"Qty_on_Asm_Component" = EXCLUDED."Qty_on_Asm_Component"
		,"Reserved_Qty_on_Inventory" = EXCLUDED."Reserved_Qty_on_Inventory"
		,"Reserved_Qty_on_Purch_Orders" = EXCLUDED."Reserved_Qty_on_Purch_Orders"
		,"EDN_Cooling_Capacity_kW" = EXCLUDED."EDN_Cooling_Capacity_kW"
		,"EDN_Heating_Capacity_kW" = EXCLUDED."EDN_Heating_Capacity_kW"
		,"EDN_Refrigerant" = EXCLUDED."EDN_Refrigerant"
		,"EDN_Refrigerant_Quantity" = EXCLUDED."EDN_Refrigerant_Quantity"
		,"EDN_Factor_Equivalent" = EXCLUDED."EDN_Factor_Equivalent"
		,"Costing_Method" = EXCLUDED."Costing_Method"
		,"Standard_Cost" = EXCLUDED."Standard_Cost"
		,"Unit_Cost" = EXCLUDED."Unit_Cost"
		,"Last_Direct_Cost" = EXCLUDED."Last_Direct_Cost"
		,"Gen_Prod_Posting_Group" = EXCLUDED."Gen_Prod_Posting_Group"
		,"Inventory_Posting_Group" = EXCLUDED."Inventory_Posting_Group"
		,"B2BSales" = EXCLUDED."B2BSales"
		,"ecom" = EXCLUDED."ecom"
		,"Vendor_No" = EXCLUDED."Vendor_No"
		,"Safety_Stock_Quantity" = EXCLUDED."Safety_Stock_Quantity"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."No"
		,NEW."PIM_number"
		,NEW."Description"
		,NEW."EDN_Description_2"
		,NEW."Type"
		,NEW."Base_Unit_of_Measure"
		,NEW."EDN_Weight_Packaging"
		,NEW."Last_Date_Modified"
		,NEW."GTIN"
		,NEW."Item_Category_Code"
		,NEW."Manufacturer_Code"
		,NEW."EDN_Brand_Code"
		,NEW."Service_Item_Group"
		,NEW."EDN_NAV_Key"
		,NEW."Inventory"
		,NEW."Qty_on_Purch_Order"
		,NEW."Qty_on_Prod_Order"
		,NEW."Qty_on_Component_Lines"
		,NEW."Qty_on_Sales_Order"
		,NEW."Qty_on_Service_Order"
		,NEW."Qty_on_Job_Order"
		,NEW."Qty_on_Assembly_Order"
		,NEW."Qty_on_Asm_Component"
		,NEW."Reserved_Qty_on_Inventory"
		,NEW."Reserved_Qty_on_Purch_Orders"
		,NEW."EDN_Cooling_Capacity_kW"
		,NEW."EDN_Heating_Capacity_kW"
		,NEW."EDN_Refrigerant"
		,NEW."EDN_Refrigerant_Quantity__x005B_UoM_x005D_"
		,NEW."EDN_Factor_Equivalent__x005B_UoM_x005D_"
		,NEW."Costing_Method"
		,NEW."Standard_Cost"
		,NEW."Unit_Cost"
		,NEW."Last_Direct_Cost"
		,NEW."Gen_Prod_Posting_Group"
		,NEW."Inventory_Posting_Group"
		,NEW."B2BSales"
		,NEW."ecom"
		,NEW."Vendor_No"
		,NEW."Safety_Stock_Quantity"
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
	grupa_tabel text := 'items'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
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