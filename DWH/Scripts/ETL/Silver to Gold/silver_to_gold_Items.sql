CREATE OR REPLACE VIEW gold.v_Zapasy AS
WITH Zapasy_Aircon AS (
	SELECT 
		i."No" AS "Nr"
		,CONCAT(i."Firma", '_', "No") AS "Klucz Nr"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "Ilosc zapasu"
		,i."unitCost" AS "Koszt jednostkowy"
		,i."baseUnitOfMeasure"  AS "Jednostka miary"
		,i."baseGroup" AS "Grupa zapasow"
		,i."ednTypeA" AS "Typ A"
		,i."ednTypeB" AS "Typ B"
		,i."costingMethod" AS "Metoda wyceny"
		,i."description" AS "Opis"
		,i."description2" AS "Opis2"
		,i."ednCoolingCapacityKW" AS "Wydajnosc chłodnicza"
		,i."ednHeatingCapacityKW" AS "Wydajnosc grzewcza" 
		,i."ednEfficiencyIndex" AS "Wskaznik wydajnosci"
		,i."ednFactorType" AS "Czynnik chlodniczy"
		,i."ednRefrigerantQuantityUoM" AS "Ilosc czynnika"
		,i."genProdPostingGroup" AS "Glowna tow grupa ksiegowa"
		,i."inventoryPostingGroup" AS "Grupa ksiegowa zapasow"
		,i."manufacturerCode" AS "Kod producenta"
--		,i."qtyAssignedToShip"
--		,i."qtyPicked"
--		,i."qtyInTransit"
--		,i."qtyOnAsmComponent"
--		,i."qtyOnAssemblyOrder"	
--		,i."qtyOnComponentLines"
--		,i."qtyOnJobOrder"	
--		,i."qtyOnProdOrder"
		,i."qtyOnPurchOrder" AS "Ilosc na zamowieniu zakupu"
--		,i."qtyOnPurchReturn"
		,i."qtyOnSalesOrder" AS "Ilosc na zamowieniu sprzedazy"
--		,i."qtyOnSalesReturn"	
		,i."qtyOnServiceOrder" AS "Ilosc na zleceniu serwisowym"	
--		,i."relOrderReceiptQty"
		,i."reservedQtyOnInventory" AS "Ilosc zarezerwowana w zapasach"
--		,i."reservedQtyOnProdOrder"	
		,i."reservedQtyOnPurchOrders" AS "Ilosc zarezerwowana w zamowieniach zakupu"
		,i."reservedQtyOnSalesOrders" AS "Ilosc zarezerwowana na zamowieniach sprzedazy"
		,'Aircon' AS "Firma"
	FROM
		silver.bc_items_aircon i
	),
	
	Zapasy_Technab AS (
	SELECT 
		i."No" AS "Nr"
		,CONCAT(i."Firma", '_', "No") AS "Klucz Nr"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "Ilosc zapasu"
		,i."unitCost" AS "Koszt jednostkowy"
		,i."baseUnitOfMeasure"  AS "Jednostka miary"
		,i."baseGroup" AS "Grupa zapasow"
		,i."ednTypeA" AS "Typ A"
		,i."ednTypeB" AS "Typ B"
		,i."costingMethod" AS "Metoda wyceny"
		,i."description" AS "Opis"
		,i."description2" AS "Opis2"
		,i."ednCoolingCapacityKW" AS "Wydajnosc chłodnicza"
		,i."ednHeatingCapacityKW" AS "Wydajnosc grzewcza" 
		,i."ednEfficiencyIndex" AS "Wskaznik wydajnosci"
		,i."ednFactorType" AS "Czynnik chlodniczy"
		,i."ednRefrigerantQuantityUoM" AS "Ilosc czynnika"
		,i."genProdPostingGroup" AS "Glowna tow grupa ksiegowa"
		,i."inventoryPostingGroup" AS "Grupa ksiegowa zapasow"
		,i."manufacturerCode" AS "Kod producenta"
--		,i."qtyAssignedToShip"
--		,i."qtyPicked"
--		,i."qtyInTransit"
--		,i."qtyOnAsmComponent"
--		,i."qtyOnAssemblyOrder"	
--		,i."qtyOnComponentLines"
--		,i."qtyOnJobOrder"	
--		,i."qtyOnProdOrder"
		,i."qtyOnPurchOrder" AS "Ilosc na zamowieniu zakupu"
--		,i."qtyOnPurchReturn"
		,i."qtyOnSalesOrder" AS "Ilosc na zamowieniu sprzedazy"
--		,i."qtyOnSalesReturn"	
		,i."qtyOnServiceOrder" AS "Ilosc na zleceniu serwisowym"	
--		,i."relOrderReceiptQty"
		,i."reservedQtyOnInventory" AS "Ilosc zarezerwowana w zapasach"
--		,i."reservedQtyOnProdOrder"	
		,i."reservedQtyOnPurchOrders" AS "Ilosc zarezerwowana w zamowieniach zakupu"
		,i."reservedQtyOnSalesOrders" AS "Ilosc zarezerwowana na zamowieniach sprzedazy"
		,'Technab' AS "Firma"
	FROM
		silver.bc_items_technab i
	),
	
	Zapasy_Zymetric AS (
	SELECT 
		i."No" AS "Nr"
		,CONCAT(i."Firma", '_', "No") AS "Klucz Nr"
		,i."no2" AS "Indeks PIM"
		,i."inventory" AS "Ilosc zapasu"
		,i."unitCost" AS "Koszt jednostkowy"
		,i."baseUnitOfMeasure"  AS "Jednostka miary"
		,i."baseGroup" AS "Grupa zapasow"
		,i."ednTypeA" AS "Typ A"
		,i."ednTypeB" AS "Typ B"
		,i."costingMethod" AS "Metoda wyceny"
		,i."description" AS "Opis"
		,i."description2" AS "Opis2"
		,i."ednCoolingCapacityKW" AS "Wydajnosc chłodnicza"
		,i."ednHeatingCapacityKW" AS "Wydajnosc grzewcza" 
		,i."ednEfficiencyIndex" AS "Wskaznik wydajnosci"
		,i."ednFactorType" AS "Czynnik chlodniczy"
		,i."ednRefrigerantQuantityUoM" AS "Ilosc czynnika"
		,i."genProdPostingGroup" AS "Glowna tow grupa ksiegowa"
		,i."inventoryPostingGroup" AS "Grupa ksiegowa zapasow"
		,i."manufacturerCode" AS "Kod producenta"
--		,i."qtyAssignedToShip"
--		,i."qtyPicked"
--		,i."qtyInTransit"
--		,i."qtyOnAsmComponent"
--		,i."qtyOnAssemblyOrder"	
--		,i."qtyOnComponentLines"
--		,i."qtyOnJobOrder"	
--		,i."qtyOnProdOrder"
		,i."qtyOnPurchOrder" AS "Ilosc na zamowieniu zakupu"
--		,i."qtyOnPurchReturn"
		,i."qtyOnSalesOrder" AS "Ilosc na zamowieniu sprzedazy"
--		,i."qtyOnSalesReturn"	
		,i."qtyOnServiceOrder" AS "Ilosc na zleceniu serwisowym"	
--		,i."relOrderReceiptQty"
		,i."reservedQtyOnInventory" AS "Ilosc zarezerwowana w zapasach"
--		,i."reservedQtyOnProdOrder"	
		,i."reservedQtyOnPurchOrders" AS "Ilosc zarezerwowana w zamowieniach zakupu"
		,i."reservedQtyOnSalesOrders" AS "Ilosc zarezerwowana na zamowieniach sprzedazy"
		,'Zymetric' AS "Firma"
	FROM
		silver.bc_items_zymetric i
	)
	
SELECT *
FROM
	Zapasy_Aircon
UNION ALL
SELECT *
FROM
	Zapasy_Technab
UNION ALL
SELECT *
FROM
	Zapasy_Zymetric
;