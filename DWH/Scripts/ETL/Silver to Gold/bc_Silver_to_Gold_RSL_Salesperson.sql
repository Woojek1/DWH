CREATE OR REPLACE VIEW gold.v_bv_salesperson_RLS as
with Struktura_Aircon as (

SELECT 
    p."Code" AS "PracownikID",
    p."Name" AS "Pracownik",
    p."E_Mail" AS "EmailPracownika",

    -- Przełożony N-3 (bezpośredni)
    s1."Name" AS "Przelozony (N-3)",
    s1."E_Mail" AS "EmailPrzelozonego (N-3)",

    -- Przełożony N-2
    s2."Name" AS "Przelozony (N-2)",
    s2."E_Mail" AS "EmailPrzelozonego (N-2)",

    -- Przełożony N-1
    s3."Name" AS "Przelozony (N-1)",
    s3."E_Mail" AS "EmailPrzelozonego (N-1)",

    -- Przełożony najwyższego szczebla (N)
    s4."Name" AS "Przelozony (N)",
    s4."E_Mail" AS "EmailPrzelozonego (N)"

FROM 
    silver.bc_salesperson_aircon p
LEFT JOIN 
    silver.bc_salesperson_aircon s1 
    ON p."EDN_Supervisor_Code" = s1."Code"
LEFT JOIN 
    silver.bc_salesperson_aircon s2 
    ON s1."EDN_Supervisor_Code" = s2."Code"
LEFT JOIN 
    silver.bc_salesperson_aircon s3 
    ON s2."EDN_Supervisor_Code" = s3."Code"
LEFT JOIN 
    silver.bc_salesperson_aircon s4 
    ON s3."EDN_Supervisor_Code" = s4."Code"
ORDER BY 
    p."Name"
),

Struktura_Technab as (
SELECT 
    p."Code" AS "PracownikID",
    p."Name" AS "Pracownik",
    p."E_Mail" AS "EmailPracownika",

    -- Przełożony N-3 (bezpośredni)
    s1."Name" AS "Przelozony (N-3)",
    s1."E_Mail" AS "EmailPrzelozonego (N-3)",

    -- Przełożony N-2
    s2."Name" AS "Przelozony (N-2)",
    s2."E_Mail" AS "EmailPrzelozonego (N-2)",

    -- Przełożony N-1
    s3."Name" AS "Przelozony (N-1)",
    s3."E_Mail" AS "EmailPrzelozonego (N-1)",

    -- Przełożony najwyższego szczebla (N)
    s4."Name" AS "Przelozony (N)",
    s4."E_Mail" AS "EmailPrzelozonego (N)"

FROM 
    silver.bc_salesperson_technab p
LEFT JOIN 
    silver.bc_salesperson_technab s1 
    ON p."EDN_Supervisor_Code" = s1."Code"
LEFT JOIN 
    silver.bc_salesperson_technab s2 
    ON s1."EDN_Supervisor_Code" = s2."Code"
LEFT JOIN 
    silver.bc_salesperson_technab s3 
    ON s2."EDN_Supervisor_Code" = s3."Code"
LEFT JOIN 
    silver.bc_salesperson_technab s4 
    ON s3."EDN_Supervisor_Code" = s4."Code"
ORDER BY 
    p."Name"
 ),
 
Struktura_Zymetric as (
SELECT 
    p."Code" AS "PracownikID",
    p."Name" AS "Pracownik",
    p."E_Mail" AS "EmailPracownika",

    -- Przełożony N-3 (bezpośredni)
    s1."Name" AS "Przelozony (N-3)",
    s1."E_Mail" AS "EmailPrzelozonego (N-3)",

    -- Przełożony N-2
    s2."Name" AS "Przelozony (N-2)",
    s2."E_Mail" AS "EmailPrzelozonego (N-2)",

    -- Przełożony N-1
    s3."Name" AS "Przelozony (N-1)",
    s3."E_Mail" AS "EmailPrzelozonego (N-1)",

    -- Przełożony najwyższego szczebla (N)
    s4."Name" AS "Przelozony (N)",
    s4."E_Mail" AS "EmailPrzelozonego (N)"

FROM 
    silver.bc_salesperson_zymetric p
LEFT JOIN 
    silver.bc_salesperson_zymetric s1 
    ON p."EDN_Supervisor_Code" = s1."Code"
LEFT JOIN 
    silver.bc_salesperson_zymetric s2 
    ON s1."EDN_Supervisor_Code" = s2."Code"
LEFT JOIN 
    silver.bc_salesperson_zymetric s3 
    ON s2."EDN_Supervisor_Code" = s3."Code"
LEFT JOIN 
    silver.bc_salesperson_zymetric s4 
    ON s3."EDN_Supervisor_Code" = s4."Code"
ORDER BY 
    p."Name"
 )
 
 select *
 from 
 	Struktura_Aircon
 union all
 select *
 from
 	Struktura_Technab
 union all
 select *
 from
 	Struktura_Zymetric