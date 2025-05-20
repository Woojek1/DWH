select
	*
from
	silver.bc_posted_sales_invoices_lines_zymetric sil
inner join
	silver.bc_posted_sales_invoices_header_zymetric sih
on sil."documentNo" = sih."No"
inner join 
	silver.bc_customers_zymetric c
on sil."sellToCustomerNo" = c."No"
inner join
	silver.bc_dimension_set_zymetric ds
on sil."dimensionSetID" = ds."dimensionSetID"


--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW  gold.v_faktury AS
WITH Faktury_Aircon AS (
	SELECT 
		CONCAT(sil."Firma", '_', sil."documentNo") AS "Klucz faktury"
		,sil."documentNo" AS "Nr faktury"
		,sil."lineNo" AS "Nr wiersza"
		,sih."Quote_No" AS "Nr oferty"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "Klucz oferty"
		,sil."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sIl."Firma", '_', sIl."shortcutDimension2Code") AS "Klucz projektu"
		,sih."Order_No" AS "Nr zamowienia"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "Klucz zamówienia"	
		,sil."postingDate" AS "Data faktury"
		,sih."VAT_Registration_No" AS "NIP"
		,sil."amount" AS "Wartosc"
		,sil."quantity" AS "Ilosc"
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "Koszt urzadzen PLN"
		,(sil."amount") - ((sil."unitCostLCY") * (sil."quantity")) AS "Zysk PLN"		
		,sil."ednSalesMargin" AS "Marza"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "Nr klienta"
		,MAX(sih."Sell_to_Customer_Name") AS "Nazwa klienta"
		,sil."amountIncludingVAT" AS "Wartosc z VAT"
		,sil."no" AS "Symbol urzadzenia"
		,sil."description2" AS "Opis urzadzenia"
		,MAX(CASE WHEN ds."dimensionCode" = 'PRACOWNIK' THEN ds."dimensionValueName" END) AS "Pracownik"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,'Aircon' AS "Firma"
	FROM
		silver.bc_posted_sales_invoices_lines_aircon sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_aircon sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_aircon c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_aircon ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
	),
	
Faktury_Technab AS (
	SELECT 
		CONCAT(sil."Firma", '_', sil."documentNo") AS "Klucz faktury"
		,sil."documentNo" AS "Nr faktury"
		,sil."lineNo" AS "Nr wiersza"
		,sih."Quote_No" AS "Nr oferty"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "Klucz oferty"
		,sil."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sIl."Firma", '_', sIl."shortcutDimension2Code") AS "Klucz projektu"
		,sih."Order_No" AS "Nr zamowienia"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "Klucz zamówienia"	
		,sil."postingDate" AS "Data faktury"
		,sih."VAT_Registration_No" AS "NIP"
		,sil."amount" AS "Wartosc"
		,sil."quantity" AS "Ilosc"
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "Koszt urzadzen PLN"
		,(sil."amount") - ((sil."unitCostLCY") * (sil."quantity")) AS "Zysk PLN"		
		,sil."ednSalesMargin" AS "Marza"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "Nr klienta"
		,MAX(sih."Sell_to_Customer_Name") AS "Nazwa klienta"
		,sil."amountIncludingVAT" AS "Wartosc z VAT"
		,sil."no" AS "Symbol urzadzenia"
		,sil."description2" AS "Opis urzadzenia"
		,MAX(CASE WHEN ds."dimensionCode" = 'PRACOWNIK' THEN ds."dimensionValueName" END) AS "Pracownik"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,'Technab' AS "Firma"
	FROM
		silver.bc_posted_sales_invoices_lines_technab sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_technab sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_technab c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_technab ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
),

Faktury_Zymetric AS (
	SELECT 
				CONCAT(sil."Firma", '_', sil."documentNo") AS "Klucz faktury"
		,sil."documentNo" AS "Nr faktury"
		,sil."lineNo" AS "Nr wiersza"
		,sih."Quote_No" AS "Nr oferty"
		,CONCAT(sil."Firma", '_', sih."Quote_No") AS "Klucz oferty"
		,sil."shortcutDimension2Code" AS "Nr projektu"
		,CONCAT(sIl."Firma", '_', sIl."shortcutDimension2Code") AS "Klucz projektu"
		,sih."Order_No" AS "Nr zamowienia"
		,CONCAT(sil."Firma", '_', sih."Order_No") AS "Klucz zamówienia"	
		,sil."postingDate" AS "Data faktury"
		,sih."VAT_Registration_No" AS "NIP"
		,sil."amount" AS "Wartosc"
		,sil."quantity" AS "Ilosc"
--		,sil."unitCostLCY" AS "Koszt urzadzenia"
		,(sil."ednOryUnitCostLCY") * (sil."quantity") AS "Koszt urzadzen PLN"
		,(sil."amount") - ((sil."unitCostLCY") * (sil."quantity")) AS "Zysk PLN"		
		,sil."ednSalesMargin" AS "Marza"
		,MAX(CONCAT(sih."Firma", '_', sih."Sell_to_Customer_No")) AS "Nr klienta"
		,MAX(sih."Sell_to_Customer_Name") AS "Nazwa klienta"
		,sil."amountIncludingVAT" AS "Wartosc z VAT"
		,sil."no" AS "Symbol urzadzenia"
		,sil."description2" AS "Opis urzadzenia"
		,MAX(CASE WHEN ds."dimensionCode" = 'PRACOWNIK' THEN ds."dimensionValueName" END) AS "Pracownik"
		,MAX(CASE WHEN ds."dimensionCode" = 'REGION' THEN ds."dimensionValueCode" END) AS "Region"
		,sil."shortcutDimension1Code" AS "MPK"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_posted_sales_invoices_lines_zymetric sil
	INNER JOIN
		silver.bc_posted_sales_invoices_header_zymetric sih
	ON sil."documentNo" = sih."No"
	INNER JOIN 
		silver.bc_customers_zymetric c
	ON sil."sellToCustomerNo" = c."No"
	INNER JOIN
		silver.bc_dimension_set_zymetric ds
	ON sil."dimensionSetID" = ds."dimensionSetID"
	GROUP BY
		sil."documentNo"
		,sil."lineNo"
		,sih."Quote_No"
		,sih."Order_No"
		,sil."postingDate"
		,sih."VAT_Registration_No"
		,sil."amount"
		,sil."unitCostLCY"
		,sil."ednSalesMargin"
		,sil."amountIncludingVAT"
		,sil."no"
		,sil."description2"
		,sil."shortcutDimension1Code"
		,sil."shortcutDimension2Code"
		,sil."Firma"
	ORDER BY
		sil."postingDate" DESC
		,sil."documentNo" ASC
		,sil."no" ASC
	)
	
SELECT *
	FROM Faktury_Aircon
UNION ALL
SELECT *
	FROM Faktury_Technab
UNION ALL
SELECT *
	FROM Faktury_Zymetric
;
