CREATE OR REPLACE VIEW gold.v_hr_enova_RLS as
with Struktura_Aircon as (
	SELECT
	    hr."PracownikID",
	    hr."Pracownik" AS "Pracownik",
	    hr."EmailHR" AS "EmailPracownika",
	
	    -- 1. Bezpośredni przełożony
	    hr1."Pracownik" AS "Przelozony (N-3)",
	    hr1."EmailHR" AS "EmailPrzelozonego (N-3)",
	
	    -- 2. Przełożony przełożonego
	    hr2."Pracownik" AS "Przelozony (N-2)",
	    hr2."EmailHR" AS "EmailPrzelozonego (N-2)",
	    
	    -- 3. Prezes    
	    hr3."Pracownik" AS "Przelozony (N-1)",
	    hr3."EmailHR" AS "EmailPrzelozonego (N-1)", 
	   
	    hr4."Pracownik" AS "Przelozony (N)",
	    hr4."EmailHR" AS "EmailPrzelozonego (N)"
	    	    	
	FROM 
		silver.hr_enova_struktura_organizacyjna_aircon hr
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr1 
	on
		hr."PrzelozonyID" = hr1."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr2
	ON 
		hr1."PrzelozonyID" = hr2."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr3
	on
		hr2."PrzelozonyID" = hr3."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr4
	on
		hr3."PrzelozonyID" = hr4."PracownikID"	
	ORDER by
		hr."Pracownik"
),

Struktura_Technab as (
	SELECT 
	    hr."PracownikID",
	    hr."Pracownik" AS "Pracownik",
	    hr."EmailHR" AS "EmailPracownika",
	
	    -- 1. Bezpośredni przełożony
	    hr1."Pracownik" AS "Przelozony (N-2)",
	    hr1."EmailHR" AS "EmailPrzelozonego (N-2)",
	
	    -- 2. Przełożony przełożonego
	    hr2."Pracownik" AS "Przelozony (N-1)",
	    hr2."EmailHR" AS "EmailPrzelozonego (N-1)",
	    
	    -- 3. Prezes    
	    hr3."Pracownik" AS "Przelozony (N)",
	    hr3."EmailHR" AS "EmailPrzelozonego (N)",
	    
	    hr4."Pracownik" AS "Przelozony (N)",
	    hr4."EmailHR" AS "EmailPrzelozonego (N)"  
	    	    	
	FROM 
		silver.hr_enova_struktura_organizacyjna_technab hr
	LEFT join
		silver.hr_enova_struktura_organizacyjna_technab hr1 
	on
		hr."PrzelozonyID" = hr1."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_technab hr2
	ON 
		hr1."PrzelozonyID" = hr2."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_technab hr3
	on
		hr2."PrzelozonyID" = hr3."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr4
	on
		hr3."PrzelozonyID" = hr4."PracownikID"	
	ORDER by
		hr."Pracownik"
),
Struktura_Zymetric as (
	SELECT 
	    hr."PracownikID",
	    hr."Pracownik" AS "Pracownik",
	    hr."EmailHR" AS "EmailPracownika",
	
	    -- 1. Bezpośredni przełożony
	    hr1."Pracownik" AS "Przelozony (N-2)",
	    hr1."EmailHR" AS "EmailPrzelozonego (N-2)",
	
	    -- 2. Przełożony przełożonego
	    hr2."Pracownik" AS "Przelozony (N-1)",
	    hr2."EmailHR" AS "EmailPrzelozonego (N-1)",
	    
	    -- 3. Prezes    
	    hr3."Pracownik" AS "Przelozony (N)",
	    hr3."EmailHR" AS "EmailPrzelozonego (N)",  
	    	    	   
	    hr4."Pracownik" AS "Przelozony (N)",
	    hr4."EmailHR" AS "EmailPrzelozonego (N)"  
	    		
	FROM 
		silver.hr_enova_struktura_organizacyjna_zymetric hr
	LEFT join
		silver.hr_enova_struktura_organizacyjna_zymetric hr1 
	on
		hr."PrzelozonyID" = hr1."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_zymetric hr2
	ON 
		hr1."PrzelozonyID" = hr2."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_zymetric hr3
	on
		hr2."PrzelozonyID" = hr3."PracownikID"
	LEFT join
		silver.hr_enova_struktura_organizacyjna_aircon hr4
	on
		hr3."PrzelozonyID" = hr4."PracownikID"	
	ORDER by
		hr."Pracownik"
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