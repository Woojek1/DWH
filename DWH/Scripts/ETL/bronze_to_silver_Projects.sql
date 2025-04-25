-- 1) USUWAMY stary trigger i funkcję
DROP TRIGGER IF EXISTS trg_after_insert_or_update_projects
  ON bronze.projects_zymetric;
DROP FUNCTION IF EXISTS bronze.fn_trigger_sync_projects();

-- 2) TWORZYMY funkcję, która dla każdego wiersza robi UPSERT do silver.projects_zymetric
CREATE OR REPLACE FUNCTION bronze.fn_upsert_project()
  RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO silver.projects_zymetric (
    "No",
    "Description",
    "Description_2",
    "Status",
    "Creation_Date",
    "Manufacturer_Code",
    "City",
    "County",
    "Object_Type",
    "Project_Source",
    "Manufacturer",
    "Planned_Delivery_Date",
    "Project_Account_Manager",
    "Salesperson_Code",
    "load_ts"
  ) VALUES (
    NEW."No",
    TRIM(NEW."Description"),
    TRIM(NEW."Description_2"),
    NEW."Status",
    NULLIF(NEW."Creation_Date", DATE '0001-01-01'),
    NEW."Manufacturer_Code",
    NEW."City",
    NEW."County",
    NEW."Object_Type",
    NEW."Project_Source",
    NEW."Manufacturer",
    NULLIF(NEW."Planned_Delivery_Date", DATE '0001-01-01'),
    NEW."Project_Account_Manager",
    NEW."Salesperson_Code",
    CURRENT_TIMESTAMP
  )
  ON CONFLICT ("No") DO UPDATE
    SET
      "Description"            = EXCLUDED."Description",
      "Description_2"          = EXCLUDED."Description_2",
      "Status"                 = EXCLUDED."Status",
      "Creation_Date"          = EXCLUDED."Creation_Date",
      "Manufacturer_Code"      = EXCLUDED."Manufacturer_Code",
      "City"                   = EXCLUDED."City",
      "County"                 = EXCLUDED."County",
      "Object_Type"            = EXCLUDED."Object_Type",
      "Project_Source"         = EXCLUDED."Project_Source",
      "Manufacturer"           = EXCLUDED."Manufacturer",
      "Planned_Delivery_Date"  = EXCLUDED."Planned_Delivery_Date",
      "Project_Account_Manager"= EXCLUDED."Project_Account_Manager",
      "Salesperson_Code"       = EXCLUDED."Salesperson_Code",
      "load_ts"                = CURRENT_TIMESTAMP
    WHERE
      silver.projects_zymetric."Description"             IS DISTINCT FROM EXCLUDED."Description"
   OR silver.projects_zymetric."Description_2"           IS DISTINCT FROM EXCLUDED."Description_2"
   OR silver.projects_zymetric."Status"                  IS DISTINCT FROM EXCLUDED."Status"
   OR silver.projects_zymetric."Creation_Date"           IS DISTINCT FROM EXCLUDED."Creation_Date"
   OR silver.projects_zymetric."Manufacturer_Code"       IS DISTINCT FROM EXCLUDED."Manufacturer_Code"
   OR silver.projects_zymetric."City"                    IS DISTINCT FROM EXCLUDED."City"
   OR silver.projects_zymetric."County"                  IS DISTINCT FROM EXCLUDED."County"
   OR silver.projects_zymetric."Object_Type"             IS DISTINCT FROM EXCLUDED."Object_Type"
   OR silver.projects_zymetric."Project_Source"          IS DISTINCT FROM EXCLUDED."Project_Source"
   OR silver.projects_zymetric."Manufacturer"            IS DISTINCT FROM EXCLUDED."Manufacturer"
   OR silver.projects_zymetric."Planned_Delivery_Date"   IS DISTINCT FROM EXCLUDED."Planned_Delivery_Date"
   OR silver.projects_zymetric."Project_Account_Manager" IS DISTINCT FROM EXCLUDED."Project_Account_Manager"
   OR silver.projects_zymetric."Salesperson_Code"        IS DISTINCT FROM EXCLUDED."Salesperson_Code";
  
  RETURN NEW;
END;
$$;

-- 3) I tworzymy nowy trigger per‐row, który wywoła tę funkcję
CREATE TRIGGER trg_after_upsert_project
  AFTER INSERT OR UPDATE
  ON bronze.projects_zymetric
  FOR EACH ROW
  EXECUTE FUNCTION bronze.fn_upsert_project();

