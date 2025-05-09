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