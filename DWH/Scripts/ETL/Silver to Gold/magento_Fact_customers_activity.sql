create or replace view gold."v_magento_Fact_customers_activity" as 
SELECT
-- v.user_id,
-- c.email,
v.event_value as SKU,
CASE v.event_type
WHEN 'view_product' THEN 'Obejrzenie karty produktu'
WHEN 'add_to_cart' THEN 'Dodanie do koszyka'
ELSE v.event_type
END as zdarzenie,
to_char(v.event_time, 'dd-mm-yyyy') as data,
bc."VATRegistrationNo" as NIP,
CASE
WHEN bc."Name" IS NULL THEN 'klient b2c'
ELSE bc."Name"
END as Nazwa,
bc."Phone_No" as Telefon,
bc."City" as Miasto,
bc."County" as Wojewodztwo
FROM
gold.v_magento_prod_event_logger AS v
LEFT JOIN
gold.v_magento_prod_customer_entity AS c
ON v.user_id = c.entity_id
LEFT JOIN
gold.v_magento_nip2 AS z
ON v.user_id = z."User_ID"
LEFT JOIN
(
SELECT
"Name",
"City",
"County",
"VATRegistrationNo",
"Phone_No", -- Corrected here: removed the trailing comma
ROW_NUMBER() OVER(PARTITION BY "VATRegistrationNo" ORDER BY "NoCustomer") as rn
FROM
gold.v_bc_customers
WHERE "VATRegistrationNo" IS NOT NULL AND "VATRegistrationNo" != ''
) AS bc
ON z."VATRegistrationNo" = bc."VATRegistrationNo" AND bc.rn = 1
WHERE
v.event_value LIKE '%MD%'
AND v.user_id NOT IN (3,4,46,62,84,211,1140,1141,1142,1145,1146,1156,1157,1158)
ORDER BY
v.event_time DESC;