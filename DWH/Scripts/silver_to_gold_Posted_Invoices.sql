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


select
	sil."documentNo" as "Nr faktury"
	,sil."lineNo" as "Nr wiersza"
	,sih."Quote_No" as "Nr oferty"
	,sil."postingDate" as "Data faktury"
	,sih."VAT_Registration_No" as "NIP"
	,sil."amount" as "Wartosc"
	,sil."ednSalesMargin" as "Marza"
	,sil."amountIncludingVAT" as "Wartosc z VAT"
	,sil."description" as "Symbol urzadzenia"
	,sil."description2" as "Opis urzadzenia"
	,max(case when ds."dimensionCode" = 'PRACOWNIK' then ds."dimensionValueName" end) as "PRACOWNIK"
	,max(case when ds."dimensionCode" = 'PROJEKT' then ds."dimensionValueName" end) as "PROJEKT"
	,max(case when ds."dimensionCode" = 'REGION' then ds."dimensionValueName" end) as "REGION"
	,sil."shortcutDimension1Code" as "MPK"
	,sil."shortcutDimension2Code" as "Nr projektu"
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
	,sil."postingDate"
	,sih."VAT_Registration_No"
	,sil."amount"
	,sil."ednSalesMargin"
	,sil."amountIncludingVAT"
	,sil."description"
	,sil."description2"
	,sil."shortcutDimension1Code"
	,sil."shortcutDimension2Code"
order by sil."postingDate" desc, sil."documentNo" asc, sil."lineNo" asc