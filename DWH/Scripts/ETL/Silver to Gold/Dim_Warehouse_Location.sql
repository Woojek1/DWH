-- gold."Dim_Warehouse_Location" source

CREATE OR REPLACE VIEW gold."Dim_Warehouse_Location"
AS WITH unioned AS (
         SELECT bc_warehouse_locations_aircon."Code" AS warehouse_code,
            bc_warehouse_locations_aircon."Key_Code" AS warehouse_key_code,
            bc_warehouse_locations_aircon."Name" AS warehouse_name,
            bc_warehouse_locations_aircon."Address" AS warehouse_address,
            bc_warehouse_locations_aircon."Post_Code" AS warehouse_post_code,
            bc_warehouse_locations_aircon."City" AS warehouse_city,
            bc_warehouse_locations_aircon."Country_Region_Code" AS warehouse_country,
            bc_warehouse_locations_aircon."Firma" AS company,
            bc_warehouse_locations_aircon.load_ts
           FROM silver.bc_warehouse_locations_aircon
        UNION ALL
         SELECT bc_warehouse_locations_technab."Code",
            bc_warehouse_locations_technab."Key_Code",
            bc_warehouse_locations_technab."Name",
            bc_warehouse_locations_technab."Address",
            bc_warehouse_locations_technab."Post_Code",
            bc_warehouse_locations_technab."City",
            bc_warehouse_locations_technab."Country_Region_Code",
            bc_warehouse_locations_technab."Firma",
            bc_warehouse_locations_technab.load_ts
           FROM silver.bc_warehouse_locations_technab
        UNION ALL
         SELECT bc_warehouse_locations_zymetric."Code",
            bc_warehouse_locations_zymetric."Key_Code",
            bc_warehouse_locations_zymetric."Name",
            bc_warehouse_locations_zymetric."Address",
            bc_warehouse_locations_zymetric."Post_Code",
            bc_warehouse_locations_zymetric."City",
            bc_warehouse_locations_zymetric."Country_Region_Code",
            bc_warehouse_locations_zymetric."Firma",
            bc_warehouse_locations_zymetric.load_ts
           FROM silver.bc_warehouse_locations_zymetric
        ), max_ts AS (
         SELECT max(unioned.load_ts) AS max_load_ts
           FROM unioned
        )
 SELECT u.warehouse_code,
    u.warehouse_key_code,
    u.warehouse_name,
    u.warehouse_address,
    u.warehouse_post_code,
    u.warehouse_city,
    u.warehouse_country,
    u.company,
    m.max_load_ts AS load_ts
   FROM unioned u
     CROSS JOIN max_ts m;