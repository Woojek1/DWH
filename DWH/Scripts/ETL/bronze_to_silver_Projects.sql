-------------------------------------------
-- CREATING POJECTS TABLE IN SILVER LAYER
-------------------------------------------


-- DROP TABLE silver.projects_zymetric;

CREATE TABLE IF NOT EXISTS silver.projects_zymetric (
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


--------------------------------------------------------------
-- CREATING DATA LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


-- DROP PROCEDURE silver.sp_load_projects_zymetric();

CREATE OR REPLACE PROCEDURE silver.sp_load_projects_zymetric()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

--Creating Projects table in Silver Layer

	CREATE TABLE IF NOT EXISTS silver.projects_zymetric (
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

--Load data into Projects table in Silver Layer

	  INSERT INTO silver.projects_zymetric (
		"No"
		,"Description"
		,"Description_2"
		,"Status"
		,"Creation_Date"
		,"Manufacturer_Code"
		,"City"
		,"County"
		,"Object_Type"
		,"Project_Source"
		,"Manufacturer"
		,"Planned_Delivery_Date"
		,"Project_Account_Manager"
		,"Salesperson_Code"
		,"load_ts"
	  )
	  SELECT
	    "No"
		,TRIM("Description") AS "Description"
		,TRIM("Description_2") AS "Description_2"
		,"Status"
		,NULLIF("Creation_Date", DATE '0001-01-01') AS "Creation_Date"
		,"Manufacturer_Code"
		,"City"
		,"County"
		,"Object_Type"
		,"Project_Source"
		,"Manufacturer"
		,NULLIF("Planned_Delivery_Date", DATE '0001-01-01') AS "Planned_Delivery_Date"
		,"Project_Account_Manager"
		,"Salesperson_Code"
	  	,CURRENT_TIMESTAMP AS "load_ts"
	  	
	  FROM bronze.projects_zymetric
	  ON CONFLICT ("No") DO UPDATE
	  SET
		"Description" = EXCLUDED."Description"
		,"Description_2" = EXCLUDED."Description_2"
		,"Status" = EXCLUDED."Status"
		,"Creation_Date" = EXCLUDED."Creation_Date"
		,"Manufacturer_Code" = EXCLUDED."Manufacturer_Code"
		,"City" = EXCLUDED."City"
		,"County" = EXCLUDED."County"
		,"Object_Type" = EXCLUDED."Object_Type"
		,"Project_Source" = EXCLUDED."Project_Source"
		,"Manufacturer" = EXCLUDED."Manufacturer"
		,"Planned_Delivery_Date" = EXCLUDED."Planned_Delivery_Date"
		,"Project_Account_Manager" = EXCLUDED."Project_Account_Manager"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"load_ts" = CURRENT_TIMESTAMP
	  WHERE 
		silver.projects_zymetric."Description" IS DISTINCT FROM EXCLUDED."Description"
	 OR silver.projects_zymetric."Description_2" IS DISTINCT FROM EXCLUDED."Description_2"
	 OR silver.projects_zymetric."Status" IS DISTINCT FROM EXCLUDED."Status"
	 OR silver.projects_zymetric."Creation_Date" IS DISTINCT FROM EXCLUDED."Creation_Date"
	 OR silver.projects_zymetric."Manufacturer_Code" IS DISTINCT FROM EXCLUDED."Manufacturer_Code"
	 OR silver.projects_zymetric."City" IS DISTINCT FROM EXCLUDED."City"
	 OR silver.projects_zymetric."County" IS DISTINCT FROM EXCLUDED."County"
	 OR silver.projects_zymetric."Object_Type" IS DISTINCT FROM EXCLUDED."Object_Type"
	 OR silver.projects_zymetric."Project_Source" IS DISTINCT FROM EXCLUDED."Project_Source"
	 OR silver.projects_zymetric."Manufacturer" IS DISTINCT FROM EXCLUDED."Manufacturer"
	 OR silver.projects_zymetric."Planned_Delivery_Date" IS DISTINCT FROM EXCLUDED."Planned_Delivery_Date"
	 OR silver.projects_zymetric."Project_Account_Manager" IS DISTINCT FROM EXCLUDED."Project_Account_Manager"
	 OR silver.projects_zymetric."Salesperson_Code" IS DISTINCT FROM EXCLUDED."Salesperson_Code";
	END;
	$procedure$
	;
	

--call silver.sp_load_projects_zymetric()


------------------------------------------------------------------------------
-- CREATING FUNCTION FOR EXECUTE LOADING PROCEDURE FROM BRONZE TO SILVER LAYER
------------------------------------------------------------------------------


-- DROP FUNCTION bronze.fn_trigger_sync_projects();

CREATE OR REPLACE FUNCTION bronze.fn_trigger_sync_projects()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  -- Wywołanie procedury ładowania danych
  EXECUTE 'CALL silver.sp_load_projects_zymetric()';
  RETURN NULL; -- nie modyfikujemy danych
END;
$function$
;


-----------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON PROJECTS TABLE
-----------------------------------------------------


CREATE TRIGGER trg_after_insert_or_update_projects
AFTER INSERT OR UPDATE
ON bronze.projects_zymetric 
FOR EACH STATEMENT EXECUTE FUNCTION bronze.fn_trigger_sync_projects()
;
