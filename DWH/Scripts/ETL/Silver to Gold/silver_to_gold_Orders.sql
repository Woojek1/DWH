select * from silver.bc_sales_lines_zymetric

-------------------------------------------------

CREATE OR REPLACE VIEW gold.v_Zamówienia AS
WITH Zamowienia_Aircon AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz zamowienia"
		,sl."documentNo" AS "Numer zamowienia"
		,sl."documentType"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,oh."Quote_No" as "Numer oferty"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Klucz oferty"
		,oh."Posting_Date" AS "Data zamowienia"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,oh."Salesperson_Code" AS "Handlowiec"
		,oh."Status" AS "Status zamowienia"
		,'Aircon' AS "Firma"
	FROM
		silver.bc_sales_lines_aircon sl
	INNER JOIN
		silver.bc_sales_orders_header_aircon oh
	ON 
		sl."documentNo" = oh."No"
),

Zamowienia_Technab AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz zamowienia"
		,sl."documentNo" AS "Numer zamowienia"
		,sl."documentType"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,oh."Quote_No" as "Numer oferty"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Klucz oferty"
		,oh."Posting_Date" AS "Data zamowienia"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,oh."Salesperson_Code" AS "Handlowiec"
		,oh."Status" AS "Status zamowienia"
		,'Technab' AS "Firma"
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_orders_header_technab oh
	ON 
		sl."documentNo" = oh."No"
	),
	
Zamowienia_Zymetric AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz zamowienia"
		,sl."documentNo" AS "Numer zamowienia"
		,sl."documentType"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,oh."Quote_No" as "Numer oferty"
		,CONCAT(sl."Firma", '_', oh."Quote_No") AS "Klucz oferty"
		,oh."Posting_Date" AS "Data zamowienia"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(oh."Firma", '_', oh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,oh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,oh."Salesperson_Code" AS "Handlowiec"
		,oh."Status" AS "Status zamowienia"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_sales_lines_zymetric sl
	INNER JOIN
		silver.bc_sales_orders_header_zymetric oh
	ON 
		sl."documentNo" = oh."No"
)

SELECT *
	FROM Zamowienia_Aircon
UNION ALL
SELECT *
	FROM Zamowienia_Technab
UNION ALL
SELECT *
	FROM Zamowienia_Zymetric
;