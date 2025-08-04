CREATE OR replace VIEW gold."Dim_BC_Vendors" AS 
SELECT *
FROM silver.bc_vendors_aircon
UNION ALL
SELECT *
FROM silver.bc_vendors_technab
UNION ALL
SELECT *
FROM silver.bc_vendors_zymetric