CREATE OR REPLACE VIEW gold.v_hr_enova_struktura_organizacyjna AS
WITH hr_Aircon AS (
	SELECT 	
		hr."PracownikID"
		,hr."PracownikKod"
		,hr."Pracownik"
		,hr."StanowiskoKod"
		,hr."StanowiskoNazwa"
		,hr."EtatStanowiskoHist"
		,hr."EtatMiejscePracyHist"
		,hr."PrzelozonyID"
		,hr."PrzelozonyKod"
		,hr."Przelozony"
		,hr."StanowiskoPrzelozKod"
		,hr."StanowiskoPrzelozNazwa"
		,hr."CzyManager"
		,hr."LiczbaPodwladnych"
		,hr."StanowiskoUmZew"
		,hr."UmZewnetrzna"
		,hr."EmailHR"
		,hr."EmailAD"
		,hr."Active"
		,hr."load_ts" AS "LoadDate"
		,'Aircon' AS "Company"
	from
		silver.hr_enova_struktura_organizacyjna_aircon hr
),

hr_Technab as (
	SELECT 	
		hr."PracownikID"
		,hr."PracownikKod"
		,hr."Pracownik"
		,hr."StanowiskoKod"
		,hr."StanowiskoNazwa"
		,hr."EtatStanowiskoHist"
		,hr."EtatMiejscePracyHist"
		,hr."PrzelozonyID"
		,hr."PrzelozonyKod"
		,hr."Przelozony"
		,hr."StanowiskoPrzelozKod"
		,hr."StanowiskoPrzelozNazwa"
		,hr."CzyManager"
		,hr."LiczbaPodwladnych"
		,hr."StanowiskoUmZew"
		,hr."UmZewnetrzna"
		,hr."EmailHR"
		,hr."EmailAD"
		,hr."Active"
		,hr."load_ts" AS "LoadDate"
		,'Technab' AS "Company"
	from
		silver.hr_enova_struktura_organizacyjna_technab hr
),

hr_Zymetric as (
	SELECT 	
		hr."PracownikID"
		,hr."PracownikKod"
		,hr."Pracownik"
		,hr."StanowiskoKod"
		,hr."StanowiskoNazwa"
		,hr."EtatStanowiskoHist"
		,hr."EtatMiejscePracyHist"
		,hr."PrzelozonyID"
		,hr."PrzelozonyKod"
		,hr."Przelozony"
		,hr."StanowiskoPrzelozKod"
		,hr."StanowiskoPrzelozNazwa"
		,hr."CzyManager"
		,hr."LiczbaPodwladnych"
		,hr."StanowiskoUmZew"
		,hr."UmZewnetrzna"
		,hr."EmailHR"
		,hr."EmailAD"
		,hr."Active"
		,hr."load_ts" AS "LoadDate"
		,'Zymetric' AS "Company"
	from
		silver.hr_enova_struktura_organizacyjna_zymetric hr
)

SELECT *
FROM
	hr_Aircon
UNION ALL
SELECT *
FROM
	hr_Technab
UNION ALL
SELECT *
FROM
	hr_Zymetric
