CREATE OR REPLACE VIEW gold.v_oferty AS
WITH Oferty_Aircon AS (
	SELECT
		sl."documentNo" AS "Nr oferty"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType" AS "Typ dokumentu"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."lineNo" AS "Linia oferty"
		,sl."no" AS "Symbol urzadzenia"
		,sl."quantity" AS "Ilosc"
		,sl."lineAmount" AS "Wartosc"
		,qh."Currency_Code" as "Kod waluty dokumentu"
		,sl."ednPriceListExchangeRate" AS "Kurs wymiany (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "Wartosc PLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chLodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Klucz nabywcy"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa nabywcy"
--		,qh."VAT_Registration_No" AS "NIP"
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
		sl."documentNo" AS "Nr oferty"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType" AS "Typ dokumentu"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."lineNo" AS "Linia oferty"
		,sl."no" AS "Symbol urzadzenia"
		,sl."quantity" AS "Ilosc"
		,sl."lineAmount" AS "Wartosc"
		,qh."Currency_Code" as "Kod waluty dokumentu"
		,sl."ednPriceListExchangeRate" AS "Kurs wymiany (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "Wartosc PLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chLodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Klucz nabywcy"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa nabywcy"
--		,qh."VAT_Registration_No" AS "NIP"
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
		sl."documentNo" AS "Nr oferty"
		,CONCAT(sl."Firma", '_', sl."documentNo") AS "Klucz oferty"
		,sl."documentType" AS "Typ dokumentu"
		,sl."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sl."Firma", '_', sl."shortcutDimension2Code") AS "Klucz projektu"
		,qh."Document_Date" AS "Data dokumentu"
		,sl."lineNo" AS "Linia oferty"
		,sl."no" AS "Symbol urzadzenia"
		,sl."quantity" AS "Ilosc"
		,sl."lineAmount" AS "Wartosc"
		,qh."Currency_Code" as "Kod waluty dokumentu"
		,sl."ednPriceListExchangeRate" AS "Kurs wymiany (do zmiany)"
		,CASE
			WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
			ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
		END AS "Wartosc PLN"
--		,sl."ednOryUnitCostLCY" as "koszt"
		,(sl."ednOryUnitCostLCY") * (sl."quantity") AS "Koszt urzadzen PLN"
		,(
			CASE
				WHEN qh."Currency_Code" = '' THEN sl."lineAmount"
				ELSE (sl."lineAmount" * sl."ednPriceListExchangeRate")
			END
		) - (sl."ednOryUnitCostLCY") * (sl."quantity")
			AS "Zysk PLN"
		,sl."ednCoolingCapacityKW" AS "Moc chLodnicza"
		,CONCAT(qh."Firma", '_', qh."Sell_to_Customer_No") AS "Klucz nabywcy"		-- Litera firmy dodana w elu utworzenia klucza klienta w każdej spółce
		,qh."Sell_to_Customer_Name" AS "Nazwa nabywcy"
--		,qh."VAT_Registration_No" AS "NIP"
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


