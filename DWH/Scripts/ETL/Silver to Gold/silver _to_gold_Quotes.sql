CREATE OR REPLACE VIEW gold.v_Oferty AS
WITH Oferty_Aircon AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" AS "Numer oferty"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,qh."Salesperson_Code" AS "Handlowiec"
		,qh."Status" AS "Status oferty"
		,qh."EDN_Quote_Type_Code" AS "Kod typu oferty"
		,'Aircon' AS "Firma"
	FROM
		silver.bc_sales_lines_aircon sl
	INNER JOIN
		silver.bc_sales_quotes_header_aircon qh
	ON 
		sl."documentNo" = qh."No"
),

Oferty_Technab AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" AS "Numer oferty"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,qh."Salesperson_Code" AS "Handlowiec"
		,qh."Status" AS "Status oferty"
		,qh."EDN_Quote_Type_Code" AS "Kod typu oferty"
		,'Technab' AS "Firma"
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_quotes_header_technab qh
	ON 
		sl."documentNo" = qh."No"
),

Oferty_Zymetric AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" AS "Numer oferty"
		,sl."lineNo" AS "Linia oferty"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."no" AS "Symbol urzadzenia"
		,sl."lineAmount" AS "Wartosc PLN"
		,sl."quantity" AS "Ilosc"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(sl."lineAmount") - ((sl."ednOryUnitCostLCY") * (sl."quantity")) AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chłodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Nr klienta"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa klienta"
		,qh."Salesperson_Code" AS "Handlowiec"
		,qh."Status" AS "Status oferty"
		,qh."EDN_Quote_Type_Code" AS "Kod typu oferty"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_sales_lines_zymetric sl
	INNER JOIN
		silver.bc_sales_quotes_header_zymetric qh
	ON 
		sl."documentNo" = qh."No"
)

SELECT *
	FROM Oferty_Aircon
UNION ALL
SELECT *
	FROM Oferty_Technab
UNION ALL
SELECT *
	FROM Oferty_Zymetric
;


