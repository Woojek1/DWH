-------------------------------------------
-- CREATING CUSTOMERS TABLE IN SILVER LAYER
-------------------------------------------


-- DROP TABLE silver.customers_zymetric;

CREATE TABLE silver.customers_zymetric (
	"No"	text    PRIMARY KEY
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
	,"EDN_Reckoning_Limit__x005B_LCY_x005D_" numeric(14, 2) NULL
	,"EDN_Used_Limit__x005B_LCY_x005D_" numeric(14, 2) NULL
	,"EDN_Factoring_Reckoning" bool NULL
	,"EDN_Insurance_Customer" bool NULL
	,"EDN_Limit_Amount_Insur__x005B_LCY_x005D_" numeric(14, 2) NULL
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
	,load_ts timestamp NULL
);


--------------------------------------------------------------
-- CREATING DATA LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


-- DROP PROCEDURE silver.sp_load_customers_zymetric(int4);

CREATE OR REPLACE PROCEDURE silver.sp_load_customers_zymetric(IN p_limit integer DEFAULT NULL::integer)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
  INSERT INTO silver.customers_zymetric (
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
	,"EDN_Reckoning_Limit__x005B_LCY_x005D_"
	,"EDN_Used_Limit__x005B_LCY_x005D_"
	,"EDN_Factoring_Reckoning"
	,"EDN_Insurance_Customer"
	,"EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
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
	,"load_ts"
  )
  SELECT
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
  	,"EDN_Reckoning_Limit__x005B_LCY_x005D_"
  	,"EDN_Used_Limit__x005B_LCY_x005D_"
  	,"EDN_Factoring_Reckoning"
  	,"EDN_Insurance_Customer"
  	,"EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
  	,"Address"
  	,"Country_Region_Code"
  	,"City"
  	,"County"
  	,"Post_Code"
  	,"EDN_Province_Code"
  	,REGEXP_REPLACE(VAT_Registration_No, '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
  	,"Gen_Bus_Posting_Group"
  	,"Payment_Terms_Code"
  	,"Payment_Method_Code"
  	,CURRENT_TIMESTAMP AS "load_ts"
  FROM bronze.customers_zymetric
  ON CONFLICT ("No") DO UPDATE
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
    ,"EDN_Reckoning_Limit__x005B_LCY_x005D_" = EXCLUDED."EDN_Reckoning_Limit__x005B_LCY_x005D_"
    ,"EDN_Used_Limit__x005B_LCY_x005D_" = EXCLUDED."EDN_Used_Limit__x005B_LCY_x005D_"
    ,"EDN_Factoring_Reckoning" = EXCLUDED."EDN_Factoring_Reckoning"
    ,"EDN_Insurance_Customer" = EXCLUDED."EDN_Insurance_Customer"
    ,"EDN_Limit_Amount_Insur__x005B_LCY_x005D_" = EXCLUDED."EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
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
    ,"load_ts" = CURRENT_TIMESTAMP
  WHERE
    silver.customers_zymetric."Contact_Type" IS DISTINCT FROM EXCLUDED."Contact_Type"
 OR silver.customers_zymetric."Name" IS DISTINCT FROM EXCLUDED."Name"
 OR silver.customers_zymetric."Name_2" IS DISTINCT FROM EXCLUDED."Name_2"
 OR silver.customers_zymetric."Search_Name" IS DISTINCT FROM EXCLUDED."Search_Name"
 OR silver.customers_zymetric."IC_Partner_Code" IS DISTINCT FROM EXCLUDED."IC_Partner_Code"
 OR silver.customers_zymetric."Balance_LCY" IS DISTINCT FROM EXCLUDED."Balance_LCY"
 OR silver.customers_zymetric."Balance_Due_LCY" IS DISTINCT FROM EXCLUDED."Balance_Due_LCY"
 OR silver.customers_zymetric."Credit_Limit_LCY" IS DISTINCT FROM EXCLUDED."Credit_Limit_LCY"
 OR silver.customers_zymetric."EDN_Black_List" IS DISTINCT FROM EXCLUDED."EDN_Black_List"
 OR silver.customers_zymetric."Salesperson_Code" IS DISTINCT FROM EXCLUDED."Salesperson_Code"
 OR silver.customers_zymetric."TotalSales2" IS DISTINCT FROM EXCLUDED."TotalSales2"
 OR silver.customers_zymetric."CustSalesLCY_CustProfit_AdjmtCostLCY" IS DISTINCT FROM EXCLUDED."CustSalesLCY_CustProfit_AdjmtCostLCY"
 OR silver.customers_zymetric."AdjCustProfit" IS DISTINCT FROM EXCLUDED."AdjCustProfit"
 OR silver.customers_zymetric."EDN_NAV_Key" IS DISTINCT FROM EXCLUDED."EDN_NAV_Key"
 OR silver.customers_zymetric."VIP" IS DISTINCT FROM EXCLUDED."VIP"
 OR silver.customers_zymetric."Company_Profile" IS DISTINCT FROM EXCLUDED."Company_Profile"
 OR silver.customers_zymetric."Company_SegmentA" IS DISTINCT FROM EXCLUDED."Company_SegmentA"
 OR silver.customers_zymetric."MagentoID" IS DISTINCT FROM EXCLUDED."MagentoID"
 OR silver.customers_zymetric."EDN_Reckoning_Limit__x005B_LCY_x005D_" IS DISTINCT FROM EXCLUDED."EDN_Reckoning_Limit__x005B_LCY_x005D_"
 OR silver.customers_zymetric."EDN_Used_Limit__x005B_LCY_x005D_" IS DISTINCT FROM EXCLUDED."EDN_Used_Limit__x005B_LCY_x005D_"
 OR silver.customers_zymetric."EDN_Factoring_Reckoning" IS DISTINCT FROM EXCLUDED."EDN_Factoring_Reckoning"
 OR silver.customers_zymetric."EDN_Insurance_Customer" IS DISTINCT FROM EXCLUDED."EDN_Insurance_Customer"
 OR silver.customers_zymetric."EDN_Limit_Amount_Insur__x005B_LCY_x005D_" IS DISTINCT FROM EXCLUDED."EDN_Limit_Amount_Insur__x005B_LCY_x005D_"
 OR silver.customers_zymetric."Address" IS DISTINCT FROM EXCLUDED."Address"
 OR silver.customers_zymetric."Country_Region_Code" IS DISTINCT FROM EXCLUDED."Country_Region_Code"
 OR silver.customers_zymetric."City" IS DISTINCT FROM EXCLUDED."City"
 OR silver.customers_zymetric."County" IS DISTINCT FROM EXCLUDED."County"
 OR silver.customers_zymetric."Post_Code" IS DISTINCT FROM EXCLUDED."Post_Code"
 OR silver.customers_zymetric."EDN_Province_Code" IS DISTINCT FROM EXCLUDED."EDN_Province_Code"
 OR silver.customers_zymetric."VAT_Registration_No" IS DISTINCT FROM EXCLUDED."VAT_Registration_No"
 OR silver.customers_zymetric."Gen_Bus_Posting_Group" IS DISTINCT FROM EXCLUDED."Gen_Bus_Posting_Group"
 OR silver.customers_zymetric."Payment_Terms_Code" IS DISTINCT FROM EXCLUDED."Payment_Terms_Code"
 OR silver.customers_zymetric."Payment_Method_Code" IS DISTINCT FROM EXCLUDED."Payment_Method_Code";
END;
$procedure$
;


call silver.sp_load_customers_zymetric()

-------------------------------------------------------------------------
-- CREATING FUNCTION FOR EXECUTE LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
-------------------------------------------------------------------------


-- DROP FUNCTION bronze.fn_trigger_sync_customers();

CREATE OR REPLACE FUNCTION bronze.fn_trigger_sync_customers()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- Wywołanie procedury ładowania danych
  EXECUTE 'CALL silver.sp_load_customers_zymetric()';
  RETURN NULL; -- nie modyfikujemy danych
END;
$function$
;


-----------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON CUSTOMER TABLE
-----------------------------------------------------


CREATE TRIGGER trg_after_insert_or_update_customers
AFTER INSERT OR UPDATE
ON bronze.customers_zymetric 
FOR EACH STATEMENT EXECUTE FUNCTION bronze.fn_trigger_sync_customers()

