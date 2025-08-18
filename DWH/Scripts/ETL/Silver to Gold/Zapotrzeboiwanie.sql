with failure_table as (

	select
		ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") as "Numer_wiersza (i)"
		,COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") AS "Ilosc_awarii (n)"
		,((ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") - 0.3)::numeric) 
			/ (COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") + 0.4)
		AS "Prawdopodobienstwo_awarii (Ti)"	
		,ln(
			"Time_To_Failure (month)"
			) 
			as "X"		
		,ln(
			-ln(1-		((ROW_NUMBER() OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB" ORDER BY fil."Time_To_Failure (month)") - 0.3)::numeric) 
			/ (COUNT(*) OVER (PARTITION BY fil."Item_No_Serviced", fil."Base_GroupB") + 0.4)
				))
				as "Y"
		,"Failure_Date"
		,"Sale_Date"
		,"Time_To_Failure (month)"
		,"Item_No_Serviced"
		,"No"
		,"Base_GroupA"
		,"Base_GroupB"
	from
		gold."Fact_Items_Failure" fil
	where
		"Time_To_Failure (month)" is not null
	and
		"Item_No_Serviced" = 'MI/AGBP-12NXD0-I'
	)
	
	select
		"Item_No_Serviced"
  		,"Base_GroupB"
  		,regr_slope("Y", "X") as "a (wsp. kierunkowy)"
  		,regr_intercept("Y", "X") as "b (wyraz wolny)" 
  		,EXP(- regr_intercept("Y","X") / regr_slope("Y","X")) AS eta
  		,max(sia."Quantity") as "Sold_quantity"
  		,max("Ilosc_awarii (n)") as "Ilosc_awarii"
  		,1-exp(
  			-power(3/(EXP(- regr_intercept("Y","X") / regr_slope("Y","X"))),
  			regr_slope("Y", "X")))
  		as "Prawdopodobieństwo wystąpienia awarii"
  		,(1-exp(
  			-power(6/(EXP(- regr_intercept("Y","X") / regr_slope("Y","X"))),
  			regr_slope("Y", "X"))))
  			*
  		max(sia."Quantity")
  		*
  		max("Ilosc_awarii (n)")
  		/
  		max(sia."Quantity") as "Zapotrzebowanie"		
	from
		failure_table ft
	left join
		gold."Dim_BC_NAV_Sold_Items_Aggregation" sia
	on ft."Item_No_Serviced" = sia."Item_No"
	group by
		"Item_No_Serviced"
		,"Base_GroupB"
	



-- Prognoza zapotrzebowania na 1 miesiąc na podstawie całej historii awarii
-- Źródło: gold."Fact_Items_Failure"
-- Wynik: 1 wiersz per Item_No_Serviced z prognozą na 1 miesiąc (prosta średnia)

WITH params AS (
    SELECT date_trunc('month', CURRENT_DATE)::date AS as_of_month
),

-- 1) Awarie zgrupowane miesięcznie po całej historii (z wyłączeniem bieżącego, niepełnego miesiąca)
failures AS (
    SELECT
        f."Item_No_Serviced"                    AS item,
        date_trunc('month', f."Failure_Date")::date AS ym,
        COUNT(*)::int                           AS cnt
    FROM gold."Fact_Items_Failure" f
    WHERE f."Failure_Date" < (SELECT as_of_month FROM params)
    GROUP BY 1,2
),

-- 2) Zakres czasowy per item: od pierwszego do ostatniego miesiąca z awarią
minmax AS (
    SELECT
        item,
        MIN(ym) AS ym_min,
        MAX(ym) AS ym_max
    FROM failures
    GROUP BY 1
),

-- 3) Siatka pełnych miesięcy per item (żeby miesiące bez awarii liczyły się jako 0)
calendar AS (
    SELECT
        m.item,
        generate_series(m.ym_min, m.ym_max, interval '1 month')::date AS ym
    FROM minmax m
),

-- 4) Uzupełnienie zerami brakujących miesięcy
filled AS (
    SELECT
        c.item,
        c.ym,
        COALESCE(f.cnt, 0) AS cnt
    FROM calendar c
    LEFT JOIN failures f
      ON f.item = c.item AND f.ym = c.ym
),

-- 5) Średnia miesięczna po całej historii
agg AS (
    SELECT
        item,
        SUM(cnt)           AS total_failures,
        COUNT(*)           AS months_observed,
        AVG(cnt::numeric)  AS avg_per_month
    FROM filled
    GROUP BY 1
)

SELECT
    item                               AS "Item_No_Serviced",
    total_failures                     AS "Total_Failures_History",
    months_observed                    AS "Months_Observed",
    ROUND(avg_per_month, 2)            AS "Forecast_1M"
FROM agg
ORDER BY "Item_No_Serviced";



------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------


-- ===========================================
-- Kolumny "jak w Excelu" + Weibull + popyt 1M
-- Źródła:
--   gold."Fact_Items_Failure" (awarie + TTF)
--   gold."Dim_BC_NAV_Sold_Items" (sprzedaże -> N_M)
-- Założenia:
--   - t = 1 miesiąc (F(t) dla 1 m-ca)
--   - Weibull liczony per Item_No_Serviced
-- ===========================================

WITH params AS (
    SELECT
        1::numeric AS t_months,                               -- horyzont t (miesiące)
        date_trunc('month', CURRENT_DATE)::date AS as_of_m
),

-- 0) Dane bazowe (filtrujemy tylko sensowne TTF > 0)
base AS (
    SELECT
        f."Failure_Date"                        AS da,              -- Data awarii D(a)
        f."Sale_Date"                           AS ds,              -- Data sprzedaży D(s)
        COALESCE(f."Time_To_Failure (month)",
                 -- awaryjnie policz TTF(m) jeśli kolumna nie byłaby w widoku
                 (
                    date_part('year',  age(f."Failure_Date", f."Sale_Date")) * 12
                  + date_part('month', age(f."Failure_Date", f."Sale_Date"))
                  + (date_part('day',  age(f."Failure_Date", f."Sale_Date")) / 30.44)
                 )::numeric
        )                                        AS ti,             -- Czas do awarii Ti (miesiące)
        f."No"                                   AS nr_c,           -- Nr C (u Ciebie "No")
        f."Base_GroupA"                          AS base_groupa,
        f."Base_GroupB"                          AS base_groupb_cn,
        f."Item_No_Serviced"                     AS item_no_serviced,
        f."Serial_No_Serviced"                   AS serial_no
    FROM gold."Fact_Items_Failure" f
    WHERE
        -- wytnij wiersze bez sprzedaży lub z nielogicznym TTF
        f."Sale_Date" IS NOT NULL
        AND (COALESCE(f."Time_To_Failure (month)", 0)) > 0
),

-- 1) Pozycje empiryczne F_i (plotting positions) per część
--    stosujemy wzór Blom'a ~ (i - 0.3) / (n + 0.4) aby uniknąć 0/1
ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.item_no_serviced ORDER BY b.ti) AS i,
        COUNT(*)    OVER (PARTITION BY b.item_no_serviced)                 AS n
    FROM base b
),

empirical AS (
    SELECT
        r.*,
        ((r.i - 0.3) / (r.n + 0.4))::numeric           AS fi_emp,   -- Empiryczne prawdopodobieństwo awarii (Fi)
        LN(r.ti)::numeric                              AS x,        -- X = ln(Ti)
        LN(-LN(1 - ((r.i - 0.3) / (r.n + 0.4))))::numeric AS y      -- Y = ln(-ln(1-Fi))
    FROM ranked r
    WHERE r.ti > 0
),

-- 2) Regresja liniowa Y ~ a*X + b per część (a=β, b = -β ln η)
fit AS (
    SELECT
        e.item_no_serviced,
        regr_slope(e.y, e.x)::numeric                  AS beta,         -- β (nachylenie)
        regr_intercept(e.y, e.x)::numeric              AS intercept,    -- b = -β ln η
        -- η = exp( -intercept / β ), ale tylko gdy beta != 0
        CASE WHEN regr_slope(e.y, e.x) IS DISTINCT FROM 0
             THEN EXP( - regr_intercept(e.y, e.x) / regr_slope(e.y, e.x) )::numeric
        END                                            AS eta
    FROM empirical e
    GROUP BY e.item_no_serviced
),

-- 3) Populacja N_M: liczba unikalnych sprzedanych sztuk danej części (do teraz)
population AS (
    SELECT
        si."Item_No"   AS item_no_serviced,
        COUNT(DISTINCT NULLIF(TRIM(si."Serial_No"),''))::numeric AS n_m
    FROM gold."Dim_BC_NAV_Sold_Items" si
    WHERE si."Posting_Date" < (SELECT as_of_m FROM params)
    GROUP BY si."Item_No"
),

-- 4) Złączenie: do każdej awarii dokładamy β, η i N_M
joined AS (
    SELECT
        e.da, e.ds, e.ti, e.nr_c, e.base_groupa, e.base_groupb_cn,
        e.item_no_serviced, e.serial_no,
        e.fi_emp, e.x, e.y,
        f.beta AS a,                     -- a = β
        f.intercept AS b,                -- b (intercept) = -β ln η
        f.beta,                          -- kolumna β jawnie
        f.eta,                           -- kolumna η jawnie
        COALESCE(p.n_m, 0) AS n_m
    FROM empirical e
    LEFT JOIN fit        f ON f.item_no_serviced = e.item_no_serviced
    LEFT JOIN population p ON p.item_no_serviced = e.item_no_serviced
)

-- 5) Wynik końcowy (kolumny jak w arkuszu)
SELECT
    -- Daty i identyfikatory
    j.da                                         AS "Data awarii D(a)",
    j.ds                                         AS "Data sprzedaży D(s)",
    ROUND(j.ti, 2)                                AS "Czas do awarii Ti",
    j.nr_c                                       AS "Nr C",
    j.base_groupa                                 AS "Base GroupA",
    j.base_groupb_cn                              AS "Base GroupB Cn",

    -- Empiryka i transformacje do wykresu Weibulla
    ROUND(j.fi_emp, 2)                            AS "Empiryczne prawdopodobieństwo awarii (Fi)",
    ROUND(j.x, 2)                                 AS "X",
    ROUND(j.y, 2)                                 AS "Y",

    -- Parametry regresji (powielone jak w Excelu: 'a', 'a wzor', 'b', 'b wzor')
    ROUND(j.a, 4)                                 AS "a",
    ROUND(j.a, 4)                                 AS "a wzor",
    ROUND(j.b, 4)                                 AS "b",
    ROUND(j.b, 4)                                 AS "b wzor",

    -- Parametry Weibulla
    ROUND(j.beta, 4)                              AS "β",
    ROUND(j.eta, 2)                               AS "η",

    -- Prawdopodobieństwo wystąpienia awarii w horyzoncie t (tu: 1 miesiąc)
    ROUND(
        CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
             THEN 1 - EXP( - POWER( ( (SELECT t_months FROM params) / j.eta ), j.beta ) )
        END
    , 2)                                          AS "Prawdopodobieństwo wystąpienia awarii F(t)",

    -- Zapotrzebowanie = N_M × F(t)   (N_M = # unikalnych sprzedanych sztuk danej części)
    ROUND(
        COALESCE(j.n_m,0)
        * CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
               THEN (1 - EXP( - POWER( ( (SELECT t_months FROM params) / j.eta ), j.beta ) ))
               ELSE NULL
          END
    , 2)                                          AS "Zapotrzebowanie"

FROM joined j
where j.nr_c = 'M/17222000031205'
ORDER BY j.item_no_serviced, j.ti;



--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------


-- ===========================================
-- Weibull per "No" (kolumna f."No")
-- Źródło: gold."Fact_Items_Failure"
-- t = 1 miesiąc (F(t))
-- ===========================================

-- Weibull per "No" + Zapotrzebowanie = N_M * F(t), t = 1 miesiąc
WITH params AS (
    SELECT 1::numeric AS t_months, date_trunc('month', CURRENT_DATE)::date AS as_of_m
),

-- 0) Dane bazowe z TTF > 0
base AS (
    SELECT
        f."Failure_Date"  AS da,
        f."Sale_Date"     AS ds,
        COALESCE(f."Time_To_Failure (month)",
            ( date_part('year',  age(f."Failure_Date", f."Sale_Date")) * 12
            + date_part('month', age(f."Failure_Date", f."Sale_Date"))
            + (date_part('day',  age(f."Failure_Date", f."Sale_Date")) / 30.44)
            )::numeric
        )                 AS ti,
        f."No"            AS no,
        f."Base_GroupA"   AS base_groupa,
        f."Base_GroupB"   AS base_groupb_cn,
        f."Item_No_Serviced" AS item_no_serviced,
        f."Serial_No_Serviced" AS serial_no
    FROM gold."Fact_Items_Failure" f
    WHERE f."Sale_Date" IS NOT NULL
      AND COALESCE(f."Time_To_Failure (month)", 0) > 0
),

-- 1) Empiryczne pozycje Fi per "No"
ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.no ORDER BY b.ti) AS i,
        COUNT(*)    OVER (PARTITION BY b.no)                AS n
    FROM base b
),
empirical AS (
    SELECT
        r.*,
        ((r.i - 0.3) / (r.n + 0.4))::numeric                    AS fi_emp,  -- Blom
        LN(r.ti)::numeric                                       AS x,
        LN(-LN(1 - ((r.i - 0.3) / (r.n + 0.4))))::numeric       AS y
    FROM ranked r
    WHERE r.ti > 0
),

-- 2) Regresja Y ~ a*X + b per "No"  (a=β,  b=-β ln η)
fit AS (
    SELECT
        e.no,
        regr_slope(e.y, e.x)::numeric     AS beta,
        regr_intercept(e.y, e.x)::numeric AS intercept,
        CASE WHEN regr_slope(e.y, e.x) IS DISTINCT FROM 0
             THEN EXP( - regr_intercept(e.y, e.x) / regr_slope(e.y, e.x) )::numeric
        END AS eta
    FROM empirical e
    GROUP BY e.no
),

-- 3) Mapowanie: w jakich modelach (Item_No) dany "No" był użyty
items_by_no AS (
    SELECT DISTINCT no, item_no_serviced AS item_no
    FROM base
),

-- 4) Populacja N_M per "No": liczba unikalnych urządzeń sprzedanych dla modeli,
--    w których ten "No" występuje (do początku bieżącego miesiąca)
population AS (
    SELECT
        m.no,
        COUNT(DISTINCT NULLIF(TRIM(si."Serial_No"), ''))::numeric AS n_m
    FROM items_by_no m
    JOIN gold."Dim_BC_NAV_Sold_Items" si
      ON si."Item_No" = m.item_no
    WHERE si."Posting_Date" < (SELECT as_of_m FROM params)
    GROUP BY m.no
),

-- 5) Połączenie wszystkiego
joined AS (
    SELECT
        e.da, e.ds, e.ti, e.no, e.base_groupa, e.base_groupb_cn,
        e.item_no_serviced, e.serial_no,
        e.fi_emp, e.x, e.y,
        f.beta AS a, f.intercept AS b, f.beta, f.eta,
        COALESCE(p.n_m, 0) AS n_m
    FROM empirical e
    LEFT JOIN fit        f ON f.no = e.no
    LEFT JOIN population p ON p.no = e.no
)

-- 6) Wynik: kolumny jak w arkuszu + F(t) i Zapotrzebowanie
SELECT
    j.da                                  AS "Data awarii D(a)",
    j.ds                                  AS "Data sprzedaży D(s)",
    ROUND(j.ti, 2)                        AS "Czas do awarii Ti",
    j.no                                  AS "Nr C",
    j.base_groupa                         AS "Base GroupA",
    j.base_groupb_cn                      AS "Base GroupB Cn",

    ROUND(j.fi_emp, 2)                    AS "Empiryczne prawdopodobieństwo awarii (Fi)",
    ROUND(j.x, 2)                         AS "X",
    ROUND(j.y, 2)                         AS "Y",

    ROUND(j.a, 4)                         AS "a",
    ROUND(j.a, 4)                         AS "a wzor",
    ROUND(j.b, 4)                         AS "b",
    ROUND(j.b, 4)                         AS "b wzor",

    ROUND(j.beta, 4)                      AS "β",
    ROUND(j.eta, 2)                       AS "η",

    -- F(t) dla t = 1 miesiąc
    ROUND(
        CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
             THEN 1 - EXP( - POWER( ((SELECT t_months FROM params) / j.eta), j.beta ) )
        END
    , 2)                                  AS "Prawdopodobieństwo wystąpienia awarii F(t)",

    -- Zapotrzebowanie = N_M * F(t)
    ROUND(
        COALESCE(j.n_m, 0) *
        CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
             THEN (1 - EXP( - POWER( ((SELECT t_months FROM params) / j.eta), j.beta ) ))
             ELSE NULL
        END
    , 2)                                  AS "Zapotrzebowanie"

FROM joined j
where j.no = 'M/11002015000051'
ORDER BY j.no, j.ti;



---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

-- ===========================================
-- Kolumny "jak w Excelu" + Weibull + popyt 1M
-- Źródła:
--   gold."Fact_Items_Failure" (awarie + TTF)
--   gold."Dim_BC_NAV_Sold_Items" (sprzedaże -> N_M)
-- Założenia:
--   - t = 1 miesiąc (F(t) dla 1 m-ca)
--   - Weibull liczony per Item_No_Serviced
-- ===========================================

WITH params AS (
    SELECT
        1::numeric AS t_months,                               -- horyzont t (miesiące)
        date_trunc('month', CURRENT_DATE)::date AS as_of_m
),

-- 0) Dane bazowe (filtrujemy tylko sensowne TTF > 0)
base AS (
    SELECT
        f."Failure_Date"                        AS da,              -- Data awarii D(a)
        f."Sale_Date"                           AS ds,              -- Data sprzedaży D(s)
        COALESCE(f."Time_To_Failure (month)",
                 (
                    date_part('year',  age(f."Failure_Date", f."Sale_Date")) * 12
                  + date_part('month', age(f."Failure_Date", f."Sale_Date"))
                  + (date_part('day',  age(f."Failure_Date", f."Sale_Date")) / 30.44)
                 )::numeric
        )                                        AS ti,             -- Czas do awarii Ti (miesiące)
        f."No"                                   AS nr_c,           -- Nr C (kolumna "No")
        f."Base_GroupA"                          AS base_groupa,
        f."Base_GroupB"                          AS base_groupb_cn,
        f."Item_No_Serviced"                     AS item_no_serviced,
        f."Serial_No_Serviced"                   AS serial_no
    FROM gold."Fact_Items_Failure" f
    WHERE
        f."Sale_Date" IS NOT NULL
        AND COALESCE(f."Time_To_Failure (month)", 0) > 0
),

-- 1) Pozycje empiryczne Fi (Blom) per część
ranked AS (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.item_no_serviced ORDER BY b.ti) AS i,
        COUNT(*)    OVER (PARTITION BY b.item_no_serviced)                 AS n
    FROM base b
),

empirical AS (
    SELECT
        r.*,
        ((r.i - 0.3) / (r.n + 0.4))::numeric           AS fi_emp,   -- Empiryczne prawdopodobieństwo awarii (Fi)
        LN(r.ti)::numeric                              AS x,        -- X = ln(Ti)
        LN(-LN(1 - ((r.i - 0.3) / (r.n + 0.4))))::numeric AS y      -- Y = ln(-ln(1-Fi))
    FROM ranked r
    WHERE r.ti > 0
),

-- 2) Regresja Y ~ a*X + b per część (a=β, b = -β ln η)
fit AS (
    SELECT
        e.item_no_serviced,
        regr_slope(e.y, e.x)::numeric                  AS beta,         -- β
        regr_intercept(e.y, e.x)::numeric              AS intercept,    -- b = -β ln η
        CASE WHEN regr_slope(e.y, e.x) IS DISTINCT FROM 0
             THEN EXP( - regr_intercept(e.y, e.x) / regr_slope(e.y, e.x) )::numeric
        END                                            AS eta            -- η
    FROM empirical e
    GROUP BY e.item_no_serviced
),

-- 3) Populacja N_M: # unikalnych sprzedanych sztuk danej części (do początku bieżącego m-ca)
population AS (
    SELECT
        si."Item_No"   AS item_no_serviced,
        COUNT(DISTINCT NULLIF(TRIM(si."Serial_No"),''))::numeric AS n_m
    FROM gold."Dim_BC_NAV_Sold_Items" si
    WHERE si."Posting_Date" < (SELECT as_of_m FROM params)
    GROUP BY si."Item_No"
),

-- 4) Złączenie
joined AS (
    SELECT
        e.da, e.ds, e.ti, e.nr_c, e.base_groupa, e.base_groupb_cn,
        e.item_no_serviced, e.serial_no,
        e.fi_emp, e.x, e.y,
        f.beta AS a,                     -- a = β
        f.intercept AS b,                -- b = -β ln η
        f.beta,                          -- β
        f.eta,                           -- η
        COALESCE(p.n_m, 0) AS n_m
    FROM empirical e
    LEFT JOIN fit        f ON f.item_no_serviced = e.item_no_serviced
    LEFT JOIN population p ON p.item_no_serviced = e.item_no_serviced
)

-- 5) Wynik końcowy
SELECT
    -- Daty i identyfikatory
    j.da                                         AS "Data awarii D(a)",
    j.ds                                         AS "Data sprzedaży D(s)",
    ROUND(j.ti, 2)                                AS "Czas do awarii Ti",
    j.nr_c                                       AS "Nr C",
    j.base_groupa                                 AS "Base GroupA",
    j.base_groupb_cn                              AS "Base GroupB Cn",
    j.item_no_serviced                            AS "Item_No_Serviced",      -- DODANE
    j.serial_no                                   AS "Serial_No_Serviced",    -- DODANE

    -- Empiryka i transformacje do wykresu Weibulla
    ROUND(j.fi_emp, 2)                            AS "Empiryczne prawdopodobieństwo awarii (Fi)",
    ROUND(j.x, 2)                                 AS "X",
    ROUND(j.y, 2)                                 AS "Y",

    -- Parametry regresji (duplikaty jak w arkuszu)
    ROUND(j.a, 4)                                 AS "a",
    ROUND(j.a, 4)                                 AS "a wzor",
    ROUND(j.b, 4)                                 AS "b",
    ROUND(j.b, 4)                                 AS "b wzor",

    -- Weibull
    ROUND(j.beta, 4)                              AS "β",
    ROUND(j.eta, 2)                               AS "η",

    -- F(t) dla t = 1 miesiąc
    ROUND(
        CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
             THEN 1 - EXP( - POWER( ((SELECT t_months FROM params) / j.eta), j.beta ) )
        END
    , 2)                                          AS "Prawdopodobieństwo wystąpienia awarii F(t)",

    -- Zapotrzebowanie = N_M × F(t)
    ROUND(
        COALESCE(j.n_m,0) *
        CASE WHEN j.beta IS NOT NULL AND j.eta IS NOT NULL AND j.eta > 0
             THEN (1 - EXP( - POWER( ((SELECT t_months FROM params) / j.eta), j.beta ) ))
             ELSE NULL
        END
    , 2)                                          AS "Zapotrzebowanie"

FROM joined j
WHERE j.nr_c = 'M/17222000031205'
ORDER BY j.item_no_serviced, j.serial_no, j.ti;
