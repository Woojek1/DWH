CREATE OR REPLACE VIEW gold.v_hr_enova_struktura_organizacyjna_RLS as
with Struktura_Aircon as (
	SELECT
	    hr."PracownikID"
	    ,hr."Pracownik" AS "Pracownik"
	    ,hr."EmailHR" AS "EmailPracownika"
	    ,coalesce(hr."EmailAD", hr."EmailHR") as "EmailAD"
	
	    -- 1. Bezpośredni przełożony
	   	,hr1."Pracownik" AS "Przelozony (N-3)"
	    ,hr1."EmailHR" AS "EmailPrzelozonego (N-3)"
	    ,coalesce(hr1."EmailAD", hr1."EmailHR") as "EmailAD (N-3)"
	    
	    -- 2. Przełożony przełożonego
	    ,hr2."Pracownik" AS "Przelozony (N-2)"
	    ,hr2."EmailHR" AS "EmailPrzelozonego (N-2)"
	    ,coalesce(hr2."EmailAD", hr2."EmailHR") as "EmailAD (N-2)"
	    
	    -- 3. Prezes    
	    ,hr3."Pracownik" AS "Przelozony (N-1)"
	    ,hr3."EmailHR" AS "EmailPrzelozonego (N-1)"
	   	,coalesce(hr3."EmailAD", hr3."EmailHR") as "EmailAD (N-1)"
	    
	    ,hr4."Pracownik" AS "Przelozony (N)"
	    ,hr4."EmailHR" AS "EmailPrzelozonego (N)"
	    ,coalesce(hr4."EmailAD", hr4."EmailHR") as "EmailAD (N)"
	    
	    ,hr4."load_ts"
	    ,'Aircon' as "Company"
	    	    	
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
	    hr."PracownikID"
	    ,hr."Pracownik" AS "Pracownik"
	    ,hr."EmailHR" AS "EmailPracownika"
	    ,coalesce(hr."EmailAD", hr."EmailHR") as "EmailAD"
	
	    -- 1. Bezpośredni przełożony
	   	,hr1."Pracownik" AS "Przelozony (N-3)"
	    ,hr1."EmailHR" AS "EmailPrzelozonego (N-3)"
	    ,coalesce(hr1."EmailAD", hr1."EmailHR") as "EmailAD (N-3)"
	    
	    -- 2. Przełożony przełożonego
	    ,hr2."Pracownik" AS "Przelozony (N-2)"
	    ,hr2."EmailHR" AS "EmailPrzelozonego (N-2)"
	    ,coalesce(hr2."EmailAD", hr2."EmailHR") as "EmailAD (N-2)"
	    
	    -- 3. Prezes    
	    ,hr3."Pracownik" AS "Przelozony (N-1)"
	    ,hr3."EmailHR" AS "EmailPrzelozonego (N-1)"
	   	,coalesce(hr3."EmailAD", hr3."EmailHR") as "EmailAD (N-1)"
	    
	    ,hr4."Pracownik" AS "Przelozony (N)"
	    ,hr4."EmailHR" AS "EmailPrzelozonego (N)"
	    ,coalesce(hr4."EmailAD", hr4."EmailHR") as "EmailAD (N)"
	    
	    ,hr1."load_ts"
	    ,'Technab' as "Company"
	   	    	
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
		silver.hr_enova_struktura_organizacyjna_technab hr4
	on
		hr3."PrzelozonyID" = hr4."PracownikID"	
	ORDER by
		hr."Pracownik"
),
Struktura_Zymetric as (
	SELECT 
	    hr."PracownikID"
	    ,hr."Pracownik" AS "Pracownik"
	    ,hr."EmailHR" AS "EmailPracownika"
	    ,coalesce(hr."EmailAD", hr."EmailHR") as "EmailAD"
	
	    -- 1. Bezpośredni przełożony
	   	,hr1."Pracownik" AS "Przelozony (N-3)"
	    ,hr1."EmailHR" AS "EmailPrzelozonego (N-3)"
	    ,coalesce(hr1."EmailAD", hr1."EmailHR") as "EmailAD (N-3)"
	    
	    -- 2. Przełożony przełożonego
	    ,hr2."Pracownik" AS "Przelozony (N-2)"
	    ,hr2."EmailHR" AS "EmailPrzelozonego (N-2)"
	    ,coalesce(hr2."EmailAD", hr2."EmailHR") as "EmailAD (N-2)"
	    
	    -- 3. Prezes    
	    ,hr3."Pracownik" AS "Przelozony (N-1)"
	    ,hr3."EmailHR" AS "EmailPrzelozonego (N-1)"
	   	,coalesce(hr3."EmailAD", hr3."EmailHR") as "EmailAD (N-1)"
	    
	    ,hr4."Pracownik" AS "Przelozony (N)"
	    ,hr4."EmailHR" AS "EmailPrzelozonego (N)"
	    ,coalesce(hr4."EmailAD", hr4."EmailHR") as "EmailAD (N)"
	    
	    ,hr1."load_ts"
	    ,'Zymetric' as "Company"
	    		
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
		silver.hr_enova_struktura_organizacyjna_zymetric hr4
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