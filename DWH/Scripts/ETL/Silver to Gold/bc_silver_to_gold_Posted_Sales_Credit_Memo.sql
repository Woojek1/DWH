CREATE OR REPLACE VIEW  gold.v_invoices as
WITH BC_Correction_Invoices_Aircon AS (
	select
		sil."DocumentNo" AS "NoInvoice"
		,CONCAT(sil."Firma", '_', sil."DocumentNo") AS "KeyNoInvoice"
		,sil."lineNo" AS "InvoiceLine"	
from 
	silver.bc_posted_sales_credit_memo_lines_aircon sil