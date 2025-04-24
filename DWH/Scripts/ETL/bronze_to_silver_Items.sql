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