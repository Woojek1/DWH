-- creating Customers table in Silver Layer

-- DROP TABLE silver.customers_zymetric;

CREATE TABLE silver.customers_zymetric (
	"No"	text    PRIMARY KEY
	,"Contact_Type" text NULL
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
	load_ts timestamp NULL
);


-- Creating Trigger in Bronze Layen on 

CREATE TRIGGER trg_after_insert_or_update_customers
AFTER INSERT OR UPDATE
ON bronze.customers_zymetric 
FOR EACH STATEMENT EXECUTE FUNCTION bronze.fn_trigger_sync_customers()

