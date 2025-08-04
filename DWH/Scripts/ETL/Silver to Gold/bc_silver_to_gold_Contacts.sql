CREATE OR replace VIEW gold."Dim_BC_Contacts" AS 
SELECT *
FROM silver.bc_contact_aircon
UNION ALL
SELECT *
FROM silver.bc_contact_technab
UNION ALL
SELECT *
FROM silver.bc_contact_zymetric