WITH sold_units AS (
    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.nav_items_ledger_entries_aircon
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''

    UNION ALL

    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.nav_items_ledger_entries_technab
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''

    UNION ALL

    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.nav_items_ledger_entries_zymetric
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''

    UNION ALL

    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.bc_items_ledger_entries_aircon
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''

    UNION ALL

    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.bc_items_ledger_entries_technab
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''

    UNION ALL

    select
    	"Entry_No"
    	,"Posting_Date"
    	,"Item_No"
    	,"Base_Group"
    	,"Serial_No"
    	,"Firma"
    from
    	silver.bc_items_ledger_entries_zymetric
    where
    	"Entry_Type" = 'Sale'
    and
    	"Serial_No" <> ''
),

duplicates AS (
    select
    	"Serial_No"
    FROM 
    	sold_units
    GROUP by
    	"Item_No"
    	,"Serial_No"

)

SELECT DISTINCT ON ("Item_No", "Serial_No") *
FROM sold_units
WHERE "Serial_No" <> ''
ORDER BY "Item_No", "Serial_No", "Posting_Date" DESC;

--SELECT su.*
--FROM sold_units su
--JOIN duplicates d ON su."Serial_No" = d."Serial_No"
--ORDER BY su."Serial_No", su."Posting_Date";
