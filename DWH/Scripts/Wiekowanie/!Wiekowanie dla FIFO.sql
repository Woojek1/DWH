---CZESC DO PRZYPISANIA DAT NA BILANSIE OTWARCIA W BC------------------
----

WITH Podstawowa_ile_BC AS(
	SELECT 
		ile."Item_No"
		,ile."Remaining_Quantity"		-- odnosi się do kolumny z Item Ledger Entry pokazuje pozostałą ilość urządzen z konkretnego przyjecia
		,ile."Serial_No"
	    ,ile."Entry_No"
--	    ,iae.[Inbound Item Entry No_]
--	    ,iae.[Outbound Item Entry No_]
--	    ,iae."Quantity"				-- odnosi się do ilości urządze we wpisie w Item Application Entry
	    ,CAST(ile."Posting_Date" AS DATE) AS "Posting_Date"
	FROM 
	    bronze.bc_items_ledger_entries_zymetric ile
--	LEFT JOIN
--	    BC_DEV_cycle.dbo.[Nabilaton$Item Ledger Entry$437dbf0e-84ff-417a-965d-ed2bb9650972] ile
--	ON iae.[Item Ledger Entry No_] = ile.[Entry No_]
	WHERE
		ile."Posting_Date" = '2024-04-01'
	AND
		ile."Serial_No" = ''
	and
		ile."Remaining_Quantity" > 0
	
),

Urzadzenia_z_NAV_poprz_data AS (
	SELECT
		"Item No_"
		,"Remaining Quantity" as "Remaining_Quantity"
		,MIN(CAST("Posting Date" AS DATE)) AS "Posting_Date"
	FROM
		bronze.nav_items_ledger_entries_zymetric
	WHERE
		"Remaining Quantity" > 0
	GROUP BY "Item No_"
			,"Remaining Quantity"
),

BO_z_BC_daty_NAV AS (
	SELECT Podstawowa_ile_BC.*
		,Urzadzenia_z_NAV_poprz_data."Posting_Date" as "Posting Date z NAV"
	FROM
		Podstawowa_ile_BC
	LEFT JOIN Urzadzenia_z_NAV_poprz_data
	ON 
	Podstawowa_ile_BC."Item_No" = Urzadzenia_z_NAV_poprz_data."Item No_"
	AND 	
		Podstawowa_ile_BC."Remaining_Quantity" = Urzadzenia_z_NAV_poprz_data."Remaining_Quantity"
	AND 
		Podstawowa_ile_BC."Posting_Date" = '2024-04-01'		-- warunek żeby szukać odpowiadającyhc ilości tylko dla urzadzen przyjetych do BC na otwarciu
),

ile_starsze_daty AS (
	SELECT 
		"Item_No"
--		,[Remaining Quantity]
		,"Serial_No"
		,"Entry_No"
--		,[Inbound Item Entry No_]
--		,[Outbound Item Entry No_]
		,"Remaining_Quantity"
--		,[Transferred-from Entry No_]
		,COALESCE ("Posting Date z NAV", "Posting_Date") AS "Starsza data"
	FROM
		BO_z_BC_daty_NAV
),

rep_fifo_na_stanie as (
	SELECT
		MIN("Item_No") AS "Item No"
		,"Entry_No" AS "Entry_No"
		,SUM("Remaining_Quantity") AS  "Total_Quantity"
		,MIN("Starsza data") AS "Wczesniejsza_data"
	FROM
		ile_starsze_daty
	GROUP BY
		"Entry_No"
	HAVING
		SUM("Remaining_Quantity") > 0
	order by 1
),

rep_fifo_agregacja_ilosci_na_stanie as (
	SELECT 
		"Item No"
		,"Wczesniejsza_data" AS "Wczesniejsza data"
		,SUM("Total_Quantity") AS "Ilosc na stanie"
	FROM
		rep_fifo_na_stanie
	GROUP BY
		"Item No"
		,"Wczesniejsza data"
),

param AS (
    SELECT DATE '2025-07-16' AS data_analizy
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

FIFO_z_wiekiem AS (

SELECT 
	BC."Item No"
	,BC."Ilosc na stanie"
	,(data_analizy) - "Wczesniejsza data" AS "WIEK"
	,CASE
		WHEN (param.data_analizy - "Wczesniejsza data")  > 2555 THEN 'Powyzej 7 lat'
		WHEN (param.data_analizy - "Wczesniejsza data")  > 1825 then '5-7 lat'
		WHEN (param.data_analizy - "Wczesniejsza data")  > 1095 then '3-5 lat'
		WHEN (param.data_analizy - "Wczesniejsza data")  > 730 then '2-3 lat'
		WHEN (param.data_analizy - "Wczesniejsza data")  > 365 then '1-2 lat'
		ELSE '0-1'
		END AS "WIEK_URZADZEN"
FROM 
	rep_fifo_agregacja_ilosci_na_stanie AS BC
	,param

)

SELECT
    FIFO_z_wiekiem."Item No"
	,max("description") AS "Opis"
	,max("description2") AS "Opis 2"
	,max("baseGroup") as "Grupa produktowa"
    ,"WIEK_URZADZEN"
    ,SUM("Ilosc na stanie") AS "Ilosc na stanie"
    ,MAX(Koszt_Urzadzen."unitCost") AS "Koszt jednostkowy"
    ,ROUND((SUM("Ilosc na stanie") * MAX(Koszt_Urzadzen."unitCost")), 2) AS "Wartosc"
FROM
    FIFO_z_wiekiem
LEFT JOIN Koszt_Urzadzen
ON Koszt_Urzadzen."No" = FIFO_z_wiekiem."Item No"
GROUP BY
    "Item No",
    "WIEK_URZADZEN"
ORDER BY 1, 4





