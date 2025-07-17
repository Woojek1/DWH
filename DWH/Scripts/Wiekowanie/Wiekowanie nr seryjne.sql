--------------------------------------------------------------------------------------------------
---TABELA URZADZEN Z BC Z DOPISANYMI DATAMI WEJSCIA NA MAGAZYN JEZELI URZADZENIE PRZESZLO Z NAV---


--WIEK URZĄDZEN Z BC ZE STANEM > O Z BUSINESS CENTRAL


WITH param AS (
    SELECT DATE '2025-07-16' AS data_analizy
),

urzadzenia_z_bc_na_stanie as (
SELECT 
    "Item_No" ,
    "Serial_No" ,
    SUM("Remaining_Quantity") AS "Ilosc na stanie" ,
    CAST(MIN("Posting_Date") AS DATE) AS "Pierwsza data" ,
    CAST(MAX("Posting_Date") AS DATE) AS "Ostatnia data" ,
    (min(data_analizy) - MIN("Posting_Date")) AS "WIEK",
    CASE 
        WHEN (min(data_analizy) - MIN("Posting_Date")) > 2555 THEN 'Powyzej 7 lat'
        WHEN (min(data_analizy) - MIN("Posting_Date")) > 1825 THEN '5-7 lat'
        WHEN (min(data_analizy) - MIN("Posting_Date")) > 1095 THEN '3-5 lat'
        WHEN (min(data_analizy) - MIN("Posting_Date")) > 730  THEN '2-3 lat'
        WHEN (min(data_analizy) - MIN("Posting_Date")) > 365  THEN '1-2 lat'
        ELSE '0-1'
    END AS "WIEK_URZADZEN"
FROM 
    bronze.bc_items_ledger_entries_zymetric, param
WHERE 
    "Serial_No" IS NOT NULL
    AND "Serial_No" <> ''
    AND "Posting_Date" <= data_analizy
--    and "Item_No" = 'MI/MI2-28Q4DN1'		---------------------------- MOZNA WPISAC SZUKANY TOWAR
GROUP BY 
    "Item_No" ,
    "Serial_No" 
HAVING 
    SUM("Remaining_Quantity") > 0
),

urzadzenia_z_nav_na_stanie as (
SELECT 
    "Item No_" ,
    "Serial No_" ,
    SUM("Remaining Quantity") AS "Ilosc na stanie" ,
    CAST(MIN("Posting Date") AS DATE) AS "Pierwsza data" ,
    CAST(MAX("Posting Date") AS DATE) AS "Ostatnia data" ,
    (min(data_analizy) - MIN("Posting Date")) AS "WIEK",
    CASE 
        WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 2555 THEN 'Powyzej 7 lat'
        WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 1825 THEN '5-7 lat'
        WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 1095 THEN '3-5 lat'
        WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 730  THEN '2-3 lat'
        WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 365  THEN '1-2 lat'
        ELSE '0-1'
    END AS "WIEK_URZADZEN"
FROM 
    bronze.nav_items_ledger_entries_zymetric, param
WHERE 
    "Serial No_" IS NOT NULL
    AND "Serial No_" <> ''
    AND "Posting Date" <= data_analizy
--    and "Item No_" = 'M/MI-71T3/DHN1-C'   ---------------------------- MOZNA WPISAC SZUKANY TOWAR
GROUP BY 
    "Item No_" ,
    "Serial No_" 
HAVING 
    SUM("Remaining Quantity") > 0
 ),
 
 Koszt_Urzadzen AS (
	SELECT
		"No"
		,"description"
		,"description2"
		,"baseGroup"
		,"unitCost"
	FROM
		bronze.bc_items_zymetric
),

Numer_seryjny_z_wiekiem AS (
SELECT 
	BC."Item_No"
	,BC."Serial_No"
	,BC."Ilosc na stanie"
	,BC."Pierwsza data" AS "Pierwsza data z BC"
	,NAV."Pierwsza data" AS "Pierwsza data z NAV"
	,BC."Ostatnia data"
	,COALESCE(NAV."Pierwsza data", BC."Pierwsza data") AS "NajwczesniejszaData"
	,(data_analizy) - COALESCE(NAV."Pierwsza data", BC."Pierwsza data") AS "WIEK"
	,CASE
		WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data"))  > 2555 THEN 'Powyzej 7 lat'
		WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data"))  > 1825 then '5-7 lat'
		WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data"))  > 1095 then '3-5 lat'
		WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data"))  > 730 then '2-3 lat'
		WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data"))  > 365 then '1-2 lat'
		ELSE '0-1'
		END AS "WIEK_URZADZEN"
FROM 
	urzadzenia_z_bc_na_stanie AS BC
LEFT JOIN 
	urzadzenia_z_nav_na_stanie AS NAV 
	on
		BC."Serial_No" = NAV."Serial No_"
	AND 
		BC."Item_No" = NAV."Item No_"
,param
)

SELECT
    "Item_No"
	,max("description") AS "Opis"
	,max("description2") AS "Opis 2"
	,max("baseGroup") as "Grupa produktowa"
    ,"WIEK_URZADZEN"
    ,SUM("Ilosc na stanie") AS "Ilosc na stanie"
    ,MAX(Koszt_Urzadzen."unitCost") AS "Koszt jednostkowy"
    ,ROUND(SUM("Ilosc na stanie") * MAX(Koszt_Urzadzen."unitCost"), 2) AS "Wartosc"
FROM
    Numer_seryjny_z_wiekiem
LEFT JOIN Koszt_Urzadzen
ON Koszt_Urzadzen."No" = Numer_seryjny_z_wiekiem."Item_No"
GROUP BY
    "Item_No",
    "WIEK_URZADZEN"
ORDER BY 1, 4




------------------------------------------------------------------
------------------------------------------------------------------

CREATE OR REPLACE FUNCTION raport_stan_urzadzen(
    firma TEXT,
    data_analizy DATE DEFAULT current_date
)
RETURNS TABLE (
    "Item_No" text,
    "Opis" text,
    "Opis 2" text,
    "Grupa produktowa" text,
    "WIEK_URZADZEN" text,
    "Ilosc na stanie" numeric,
    "Koszt jednostkowy" numeric,
    "Wartosc" numeric
) AS $$
DECLARE
    sql text;
BEGIN
    sql := format($f$
        WITH param AS (
            SELECT %L::date AS data_analizy
        ),
        urzadzenia_z_bc_na_stanie AS (
            SELECT 
                "Item_No",
                "Serial_No",
                SUM("Remaining_Quantity") AS "Ilosc na stanie",
                CAST(MIN("Posting_Date") AS DATE) AS "Pierwsza data",
                CAST(MAX("Posting_Date") AS DATE) AS "Ostatnia data",
                (min(data_analizy) - MIN("Posting_Date")) AS "WIEK",
                CASE 
                    WHEN (min(data_analizy) - MIN("Posting_Date")) > 2555 THEN 'Powyzej 7 lat'
                    WHEN (min(data_analizy) - MIN("Posting_Date")) > 1825 THEN '5-7 lat'
                    WHEN (min(data_analizy) - MIN("Posting_Date")) > 1095 THEN '3-5 lat'
                    WHEN (min(data_analizy) - MIN("Posting_Date")) > 730  THEN '2-3 lat'
                    WHEN (min(data_analizy) - MIN("Posting_Date")) > 365  THEN '1-2 lat'
                    ELSE '0-1'
                END AS "WIEK_URZADZEN"
            FROM 
                bronze.bc_items_ledger_entries_%I, param
            WHERE 
                "Serial_No" IS NOT NULL
                AND "Serial_No" <> ''
                AND "Posting_Date" <= data_analizy
            GROUP BY 
                "Item_No",
                "Serial_No"
            HAVING 
                SUM("Remaining_Quantity") > 0
        ),
        urzadzenia_z_nav_na_stanie AS (
            SELECT 
                "Item No_",
                "Serial No_",
                SUM("Remaining Quantity") AS "Ilosc na stanie",
                CAST(MIN("Posting Date") AS DATE) AS "Pierwsza data",
                CAST(MAX("Posting Date") AS DATE) AS "Ostatnia data",
                (min(data_analizy) - MIN("Posting Date")) AS "WIEK",
                CASE 
                    WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 2555 THEN 'Powyzej 7 lat'
                    WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 1825 THEN '5-7 lat'
                    WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 1095 THEN '3-5 lat'
                    WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 730  THEN '2-3 lat'
                    WHEN (min(data_analizy) - MIN("Posting Date"::date)) > 365  THEN '1-2 lat'
                    ELSE '0-1'
                END AS "WIEK_URZADZEN"
            FROM 
                bronze.nav_items_ledger_entries_%I, param
            WHERE 
                "Serial No_" IS NOT NULL
                AND "Serial No_" <> ''
                AND "Posting Date" <= data_analizy
            GROUP BY 
                "Item No_",
                "Serial No_"
            HAVING 
                SUM("Remaining Quantity") > 0
        ),
        Koszt_Urzadzen AS (
            SELECT
                "No",
                "description",
                "description2",
                "baseGroup",
                "unitCost"
            FROM
                bronze.bc_items_%I
        ),
        Numer_seryjny_z_wiekiem AS (
            SELECT 
                BC."Item_No",
                BC."Serial_No",
                BC."Ilosc na stanie",
                BC."Pierwsza data" AS "Pierwsza data z BC",
                NAV."Pierwsza data" AS "Pierwsza data z NAV",
                BC."Ostatnia data",
                COALESCE(NAV."Pierwsza data", BC."Pierwsza data") AS "NajwczesniejszaData",
                (data_analizy) - COALESCE(NAV."Pierwsza data", BC."Pierwsza data") AS "WIEK",
                CASE
                    WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data")) > 2555 THEN 'Powyzej 7 lat'
                    WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data")) > 1825 THEN '5-7 lat'
                    WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data")) > 1095 THEN '3-5 lat'
                    WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data")) > 730  THEN '2-3 lat'
                    WHEN (param.data_analizy - COALESCE(NAV."Pierwsza data", BC."Pierwsza data")) > 365  THEN '1-2 lat'
                    ELSE '0-1'
                END AS "WIEK_URZADZEN"
            FROM 
                urzadzenia_z_bc_na_stanie AS BC
            LEFT JOIN 
                urzadzenia_z_nav_na_stanie AS NAV 
                ON BC."Serial_No" = NAV."Serial No_"
                AND BC."Item_No" = NAV."Item No_"
            ,param
        )
        SELECT
            "Item_No",
            max("description") AS "Opis",
            max("description2") AS "Opis 2",
            max("baseGroup") AS "Grupa produktowa",
            "WIEK_URZADZEN",
            SUM("Ilosc na stanie") AS "Ilosc na stanie",
            MAX(Koszt_Urzadzen."unitCost") AS "Koszt jednostkowy",
            ROUND(SUM("Ilosc na stanie") * MAX(Koszt_Urzadzen."unitCost"), 2) AS "Wartosc"
        FROM
            Numer_seryjny_z_wiekiem
        LEFT JOIN Koszt_Urzadzen
            ON Koszt_Urzadzen."No" = Numer_seryjny_z_wiekiem."Item_No"
        GROUP BY
            "Item_No",
            "WIEK_URZADZEN"
        ORDER BY 1, 4
    $f$, data_analizy, firma, firma, firma);

    RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM raport_stan_urzadzen('technab')
