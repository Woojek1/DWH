CREATE OR REPLACE VIEW gold."Fact_Reservation"
AS WITH unioned AS (
         SELECT bc_reservation_entry_aircon."Entry_No" AS reservation_no,
            bc_reservation_entry_aircon."Key_Entry_No" AS reservation_key_no,
            bc_reservation_entry_aircon."Reservation_Status" AS reservation_status,
            bc_reservation_entry_aircon."Source_Type" AS reservation_type,
            bc_reservation_entry_aircon."Key_Item_No" AS item_key_no,
            bc_reservation_entry_aircon."Quantity_Base" AS quantity_reserved,
            bc_reservation_entry_aircon."Location_Code" AS location_code,
            bc_reservation_entry_aircon."Shipment_Date" AS shipment_date,
            bc_reservation_entry_aircon."Creation_Date" AS reservation_date,
            bc_reservation_entry_aircon."Firma" AS company,
            bc_reservation_entry_aircon.load_ts
           FROM silver.bc_reservation_entry_aircon
          WHERE bc_reservation_entry_aircon."Reservation_Status" = 'Reservation'::text
        UNION ALL
         SELECT bc_reservation_entry_technab."Entry_No",
            bc_reservation_entry_technab."Key_Entry_No",
            bc_reservation_entry_technab."Reservation_Status",
            bc_reservation_entry_technab."Source_Type",
            bc_reservation_entry_technab."Key_Item_No",
            bc_reservation_entry_technab."Quantity_Base",
            bc_reservation_entry_technab."Location_Code",
            bc_reservation_entry_technab."Shipment_Date",
            bc_reservation_entry_technab."Creation_Date",
            bc_reservation_entry_technab."Firma",
            bc_reservation_entry_technab.load_ts
           FROM silver.bc_reservation_entry_technab
          WHERE bc_reservation_entry_technab."Reservation_Status" = 'Reservation'::text
        UNION ALL
         SELECT bc_reservation_entry_zymetric."Entry_No",
            bc_reservation_entry_zymetric."Key_Entry_No",
            bc_reservation_entry_zymetric."Reservation_Status",
            bc_reservation_entry_zymetric."Source_Type",
            bc_reservation_entry_zymetric."Key_Item_No",
            bc_reservation_entry_zymetric."Quantity_Base",
            bc_reservation_entry_zymetric."Location_Code",
            bc_reservation_entry_zymetric."Shipment_Date",
            bc_reservation_entry_zymetric."Creation_Date",
            bc_reservation_entry_zymetric."Firma",
            bc_reservation_entry_zymetric.load_ts
           FROM silver.bc_reservation_entry_zymetric
          WHERE bc_reservation_entry_zymetric."Reservation_Status" = 'Reservation'::text
        ), max_ts AS (
         SELECT max(unioned.load_ts) AS max_load_ts
           FROM unioned
        )
 SELECT u.reservation_no,
    u.reservation_key_no,
    u.reservation_type,
    u.item_key_no,
    u.quantity_reserved,
    u.location_code,
    u.shipment_date,
    u.reservation_date,
    u.company,
    m.max_load_ts AS load_ts
   FROM unioned u
     CROSS JOIN max_ts m;


