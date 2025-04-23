-------------------------------------------
-- CREATING POJECTS TABLE IN SILVER LAYER
-------------------------------------------


-- DROP TABLE silver.projects_zymetric;

CREATE TABLE silver.projects_zymetric (
	"No" text PRIMARY KEY
	,"Description" text NULL
	,"Description_2" text NULL
	,"Status" text NULL
	,"Creation_Date" date NULL
	,"Manufacturer_Code" text NULL
	,"City" text NULL
	,"County" text NULL
	,"Object_Type" text NULL
	,"Project_Source" text NULL
	,"Manufacturer" text NULL
	,"Planned_Delivery_Date" date NULL
	,"Project_Account_Manager" text NULL
	,"Salesperson_Code" text NULL
	,"load_ts" timestamptz NULL
);