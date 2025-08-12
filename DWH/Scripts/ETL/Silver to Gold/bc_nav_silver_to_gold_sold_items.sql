create or replace view gold."Dim_BC_NAV_Sold_Items" as

-- BC AIRCON

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.bc_items_ledger_entries_aircon
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1
	
union all

-- BC TECHNAB

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.bc_items_ledger_entries_technab
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1
	
union all

-- BC ZYMETRIC

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.bc_items_ledger_entries_zymetric
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1
	
union all

-- NAV AIRCON

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.nav_items_ledger_entries_aircon
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1
	
union all

-- NAV TECHNAB

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.nav_items_ledger_entries_technab
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1

union all

SELECT
    "Entry_No",
    "Posting_Date",
    "Entry_Type",
    "Item_No",
    "Serial_No",
    "Firma"
FROM (
    SELECT
        "Entry_No",
        "Posting_Date",
        "Entry_Type",
        "Item_No",
        "Serial_No",
        "Firma",
        ROW_NUMBER() OVER (
            PARTITION BY "Item_No", "Serial_No"
            ORDER BY "Posting_Date" DESC
        ) AS rn
    FROM
        silver.nav_items_ledger_entries_zymetric
    WHERE
        "Entry_Type" = 'Sale'
) t
WHERE rn = 1

	
	
