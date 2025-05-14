select * from silver.bc_sales_lines_zymetric

-------------------------------------------------

CREATE OR REPLACE VIEW gold.v_Oferty AS
WITH Oferty_Aircon AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") as "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" as "Numer oferty"
		,sl."lineNo" as "Linia oferty"
		,sl."shortcutDimension2Code" as "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") as "Klucz projektu"
		,qh."Document_Date" as "Data dokumentu"
		,sl."no" as "Symbol urzadzenia"
		,sl."lineAmount" as "Wartosc PLN"
		,sl."ednCoolingCapacityKW" as "Moc chłodnicza"	
	FROM
		silver.bc_sales_lines_aircon sl
	INNER JOIN
		silver.bc_sales_quotes_header_aircon qh
	ON 
		sl."documentNo" = qh."No"
),

Oferty_Technab as (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") as "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" as "Numer oferty"
		,sl."lineNo" as "Linia oferty"
		,sl."shortcutDimension2Code" as "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") as "Klucz projektu"
		,qh."Document_Date" as "Data dokumentu"
		,sl."no" as "Symbol urzadzenia"
		,sl."lineAmount" as "Wartosc PLN"
		,sl."ednCoolingCapacityKW" as "Moc chłodnicza"	
	FROM
		silver.bc_sales_lines_technab sl
	INNER JOIN
		silver.bc_sales_quotes_header_technab qh
	ON 
		sl."documentNo" = qh."No"
),

Oferty_Zymetric AS (
	SELECT
		CONCAT(sl."Firma", '_', sl."documentNo") as "Klucz oferty"
		,sl."documentType"
		,sl."documentNo" as "Numer oferty"
		,sl."lineNo" as "Linia oferty"
		,sl."shortcutDimension2Code" as "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") as "Klucz projektu"
		,qh."Document_Date" as "Data dokumentu"
		,sl."no" as "Symbol urzadzenia"
		,sl."lineAmount" as "Wartosc PLN"
		,sl."ednCoolingCapacityKW" as "Moc chłodnicza"	
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