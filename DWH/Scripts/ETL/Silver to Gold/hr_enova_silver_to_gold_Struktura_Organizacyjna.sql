SELECT 
    p."PracownikID",
    p."Pracownik" AS "Pracownik",
    p."EmailHR" AS "EmailPracownika",

    -- 1. Bezpośredni przełożony
    p1."Pracownik" AS "Przelozony (N-2)",
    p1."EmailHR" AS "EmailPrzelozonego (N-2)",

    -- 2. Przełożony przełożonego
    p2."Pracownik" AS "Przelozony (N-1)",
    p2."EmailHR" AS "EmailPrzelozonego (N-1)",
    
    -- 3. Prezes    
    p3."Pracownik" AS "Przelozony (N)",
    p3."EmailHR" AS "EmailPrzelozonego (N)"  

FROM 
	bronze.hr_enova_struktura_organizacyjna_zymetric hr
LEFT join
	bronze.hr_enova_struktura_organizacyjna_zymetric hr1 
on
	p."PrzelozonyID" = hr1."PracownikID"
LEFT join
	bronze.hr_enova_struktura_organizacyjna_zymetric hr2
ON 
	p1."PrzelozonyID" = hr2."PracownikID"
LEFT join
	bronze.hr_enova_struktura_organizacyjna_zymetric hr3
on
	p2."PrzelozonyID" = hr3."PracownikID"
ORDER by
	p."Pracownik"