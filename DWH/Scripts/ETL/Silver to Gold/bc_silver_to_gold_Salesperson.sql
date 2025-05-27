CREATE OR REPLACE VIEW gold.v_bc_customers AS
WITH Salespersons_Aircon AS (
	SELECT 	
		sp."Code" as "Code"
		,concat(sp."Firma", '_', sp."Code") as "KeyNoCustomer"
		,sp."Name" as "Name"
		,sp."E_Mail" as "Email"
		,sp."Job_Title" as "JobTitle"
		,sp."EDN_Supervisor_Code" as "SupervisorCode"
		,sp."load_ts" AS "LoadDate"
		,'Aircon' AS "Company"
	from
		silver.bc_salesperson_aircon sp
),

Salespersons_Technab as (
	SELECT 	
		sp."Code" as "Code"
		,concat(sp."Firma", '_', sp."Code") as "KeyNoCustomer"
		,sp."Name" as "Name"
		,sp."E_Mail" as "Email"
		,sp."Job_Title" as "JobTitle"
		,sp."EDN_Supervisor_Code" as "SupervisorCode"
		,sp."load_ts" AS "LoadDate"
		,'Technab' AS "Company"
	from
		silver.bc_salesperson_technab sp
),

Salespersons_Zymetric as (
	SELECT 	
		sp."Code" as "Code"
		,concat(sp."Firma", '_', sp."Code") as "KeyNoCustomer"
		,sp."Name" as "Name"
		,sp."E_Mail" as "Email"
		,sp."Job_Title" as "JobTitle"
		,sp."EDN_Supervisor_Code" as "SupervisorCode"
		,sp."load_ts" AS "LoadDate"
		,'Zymetric' AS "Company"
	from
		silver.bc_salesperson_zymetric sp
)

SELECT *
FROM
	Salespersons_Aircon
UNION ALL
SELECT *
FROM
	Salespersons_Technab
UNION ALL
SELECT *
FROM
	Salespersons_Zymetric
;