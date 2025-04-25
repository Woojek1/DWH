-------------------------------------------
-- CREATING ITEMS TABLE IN SILVER LAYER
-------------------------------------------


-- DROP TABLE silver.items_zymetric;

CREATE TABLE IF NOT EXISTS silver.items_zymetric (
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
	,"EDN_Refrigerant_Quantity__x005B_UoM_x005D_" numeric(14, 4) NULL
	,"EDN_Factor_Equivalent__x005B_UoM_x005D_" numeric(14, 4) NULL
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
	,"load_ts" timestamptz NULL
);


--------------------------------------------------------------
-- CREATING DATA LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


-- DROP PROCEDURE silver.sp_load_items_zymetric();

CREATE OR REPLACE PROCEDURE silver.sp_load_items_zymetric()
LANGUAGE plpgsql
AS $procedure$
BEGIN

--Creating Items table in Silver Layer
	
	CREATE TABLE IF NOT EXISTS silver.items_zymetric (
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
		,"EDN_Refrigerant_Quantity__x005B_UoM_x005D_" numeric(14, 4) NULL
		,"EDN_Factor_Equivalent__x005B_UoM_x005D_" numeric(14, 4) NULL
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
		,"load_ts" timestamptz NULL
	);
	
--Load data into Items table in Silver Layer

	INSERT INTO silver.items_zymetric (
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
		,"EDN_Refrigerant_Quantity__x005B_UoM_x005D_"
		,"EDN_Factor_Equivalent__x005B_UoM_x005D_"
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
		,"load_ts"
	)
	SELECT
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
		,"EDN_Refrigerant_Quantity__x005B_UoM_x005D_"
		,"EDN_Factor_Equivalent__x005B_UoM_x005D_"
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
		,CURRENT_TIMESTAMP AS "load_ts"
	FROM bronze.items_zymetric
	ON CONFLICT ("No") DO UPDATE
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
		,"EDN_Refrigerant_Quantity__[UoM]" = EXCLUDED."EDN_Refrigerant_Quantity__[UoM]"
		,"EDN_Factor_Equivalent__[UoM]" = EXCLUDED."EDN_Factor_Equivalent__[UoM]"
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
		,"load_ts" = CURRENT_TIMESTAMP
		WHERE
			silver.items_zymetric."PIM_number" IS DISTINCT FROM EXCLUDED."PIM_number"
		OR silver.items_zymetric."Description" IS DISTINCT FROM EXCLUDED."Description"
		OR silver.items_zymetric."EDN_Description_2" IS DISTINCT FROM EXCLUDED."EDN_Description_2"
		OR silver.items_zymetric."Type" IS DISTINCT FROM EXCLUDED."Type"
		OR silver.items_zymetric."Base_Unit_of_Measure" IS DISTINCT FROM EXCLUDED."Base_Unit_of_Measure"
		OR silver.items_zymetric."EDN_Weight_Packaging" IS DISTINCT FROM EXCLUDED."EDN_Weight_Packaging"
		OR silver.items_zymetric."Last_Date_Modified" IS DISTINCT FROM EXCLUDED."Last_Date_Modified"
		OR silver.items_zymetric."GTIN" IS DISTINCT FROM EXCLUDED."GTIN"
		OR silver.items_zymetric."Item_Category_Code" IS DISTINCT FROM EXCLUDED."Item_Category_Code"
		OR silver.items_zymetric."Manufacturer_Code" IS DISTINCT FROM EXCLUDED."Manufacturer_Code"
		OR silver.items_zymetric."EDN_Brand_Code" IS DISTINCT FROM EXCLUDED."EDN_Brand_Code"
		OR silver.items_zymetric."Service_Item_Group" IS DISTINCT FROM EXCLUDED."Service_Item_Group"
		OR silver.items_zymetric."EDN_NAV_Key" IS DISTINCT FROM EXCLUDED."EDN_NAV_Key"
		OR silver.items_zymetric."Inventory" IS DISTINCT FROM EXCLUDED."Inventory"
		OR silver.items_zymetric."Qty_on_Purch_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Purch_Order"
		OR silver.items_zymetric."Qty_on_Prod_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Prod_Order"
		OR silver.items_zymetric."Qty_on_Component_Lines" IS DISTINCT FROM EXCLUDED."Qty_on_Component_Lines"
		OR silver.items_zymetric."Qty_on_Sales_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Sales_Order"
		OR silver.items_zymetric."Qty_on_Service_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Service_Order"
		OR silver.items_zymetric."Qty_on_Job_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Job_Order"
		OR silver.items_zymetric."Qty_on_Assembly_Order" IS DISTINCT FROM EXCLUDED."Qty_on_Assembly_Order"
		OR silver.items_zymetric."Qty_on_Asm_Component" IS DISTINCT FROM EXCLUDED."Qty_on_Asm_Component"
		OR silver.items_zymetric."Reserved_Qty_on_Inventory" IS DISTINCT FROM EXCLUDED."Reserved_Qty_on_Inventory"
		OR silver.items_zymetric."Reserved_Qty_on_Purch_Orders" IS DISTINCT FROM EXCLUDED."Reserved_Qty_on_Purch_Orders"
		OR silver.items_zymetric."EDN_Cooling_Capacity_kW" IS DISTINCT FROM EXCLUDED."EDN_Cooling_Capacity_kW"
		OR silver.items_zymetric."EDN_Heating_Capacity_kW" IS DISTINCT FROM EXCLUDED."EDN_Heating_Capacity_kW"
		OR silver.items_zymetric."EDN_Refrigerant" IS DISTINCT FROM EXCLUDED."EDN_Refrigerant"
		OR silver.items_zymetric."EDN_Refrigerant_Quantity__[UoM]" IS DISTINCT FROM EXCLUDED."EDN_Refrigerant_Quantity__[UoM]"
		OR silver.items_zymetric."EDN_Factor_Equivalent__[UoM]" IS DISTINCT FROM EXCLUDED."EDN_Factor_Equivalent__[UoM]"
		OR silver.items_zymetric."Costing_Method" IS DISTINCT FROM EXCLUDED."Costing_Method"
		OR silver.items_zymetric."Standard_Cost" IS DISTINCT FROM EXCLUDED."Standard_Cost"
		OR silver.items_zymetric."Unit_Cost" IS DISTINCT FROM EXCLUDED."Unit_Cost"
		OR silver.items_zymetric."Last_Direct_Cost" IS DISTINCT FROM EXCLUDED."Last_Direct_Cost"
		OR silver.items_zymetric."Gen_Prod_Posting_Group" IS DISTINCT FROM EXCLUDED."Gen_Prod_Posting_Group"
		OR silver.items_zymetric."Inventory_Posting_Group" IS DISTINCT FROM EXCLUDED."Inventory_Posting_Group"
		OR silver.items_zymetric."B2BSales" IS DISTINCT FROM EXCLUDED."B2BSales"
		OR silver.items_zymetric."ecom" IS DISTINCT FROM EXCLUDED."ecom"
		OR silver.items_zymetric."Vendor_No" IS DISTINCT FROM EXCLUDED."Vendor_No"
		OR silver.items_zymetric."Safety_Stock_Quantity" IS DISTINCT FROM EXCLUDED."Safety_Stock_Quantity";
	END;
	$procedure$
	;
	
	
	--call silver.sp_load_items_zymetric()
	
	
------------------------------------------------------------------------------
-- CREATING FUNCTION FOR EXECUTE LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
------------------------------------------------------------------------------


-- DROP FUNCTION bronze.fn_trigger_sync_items();

CREATE OR REPLACE FUNCTION bronze.fn_trigger_sync_items()
	RETURNS trigger
	LANGUAGE plpgsql
AS $function$
BEGIN
-- Wywołanie procedury ładowania danych
	EXECUTE 'CALL silver.sp_load_items_zymetric()';
	RETURN NULL; -- nie modyfikujemy danych
END;
$function$
;


-----------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON ITEMS TABLE
-----------------------------------------------------


CREATE TRIGGER trg_after_insert_or_update_items
AFTER INSERT OR UPDATE
ON bronze.items_zymetric 
FOR EACH STATEMENT EXECUTE FUNCTION bronze.fn_trigger_sync_items()
;