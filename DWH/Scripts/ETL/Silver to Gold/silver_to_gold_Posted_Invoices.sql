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

create or replace view gold.v_faktury AS
select
	sil."documentNo" as "Nr faktury"
	,sil."lineNo" as "Nr wiersza"
	,sih."Quote_No" as "Nr oferty"
	,sih."Order_No" as "Nr zamowienia"
	,sil."postingDate" as "Data faktury"
	,sih."VAT_Registration_No" as "NIP"
	,sil."amount" as "Wartosc"
	,sil."unitCostLCY" as "Koszt urzadzenia"
	,sil."ednSalesMargin" as "Marza"
	,sil."amountIncludingVAT" as "Wartosc z VAT"
	,sil."no" as "Symbol urzadzenia"
	,sil."description2" as "Opis urzadzenia"
	,max(case when ds."dimensionCode" = 'PRACOWNIK' then ds."dimensionValueName" end) as "PRACOWNIK"
	,max(case when ds."dimensionCode" = 'PROJEKT' then ds."dimensionValueName" end) as "PROJEKT"
	,max(case when ds."dimensionCode" = 'REGION' then ds."dimensionValueName" end) as "REGION"
	,sil."shortcutDimension1Code" as "MPK"
	,sil."shortcutDimension2Code" as "Nr projektu"
	,sil."Firma" as "Firma"
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
order by
	sil."postingDate" desc
	,sil."documentNo" asc
	,sil."no" asc
;