SELECT 
    prod_start,
    MAX(prod_end) AS prod_end,
    MAX(source) As source,
    MAX(country) AS country,
    MAX(tenor) AS tenor,
    CASE 
        WHEN production_type IN ('WIND_ONSHORE', 'WIND_OFFSHORE') THEN 'WIND'
        ELSE production_type
    END AS production_type,
    SUM(quantity) AS quantity,
    MAX(unit) AS unit,
    MAX(processing) AS processing
FROM 
    production_per_type
WHERE 
    tenor = 'PT1H'
    AND source = 'RTE' 
    AND country = 'FR'
    AND production_type IN ('SOLAR', 'WIND_ONSHORE', 'WIND_OFFSHORE')
GROUP BY
    prod_start,
    production_type;


SELECT 
    * 
FROM 
    installed_capacities 
WHERE 
    source = 'ODRE'
    AND country = 'FR'
    AND capacities_type IN ('SOLAR', 'WIND');

INSERT INTO capacity_factor_per_type (
    cf_start,
    cf_end,
    source,
    country,
    tenor,
    cf_type,
    capacity_factor,
    processing)
SELECT
    p.prod_start AS cf_start,
    p.prod_end AS cf_end,
    'prod' || p.source || 'capa'|| c.source AS source,
    p.country AS country,
    p.tenor AS tenor,
    p.production_type_modify AS cf_type,
    p.quantity / c.quantity AS capacity_factor,
    p.processing AS processing
FROM (
    SELECT 
        prod_start,
        MAX(prod_end) AS prod_end,
        MAX(source) AS source,
        MAX(country) AS country,
        MAX(tenor) AS tenor,
        CASE 
            WHEN production_type IN ('WIND_ONSHORE', 'WIND_OFFSHORE') THEN 'WIND'
            ELSE production_type
        END AS production_type_modify,
        SUM(quantity) AS quantity,
        MAX(unit) AS unit,
        MAX(processing) AS processing
    FROM 
        production_per_type
    WHERE 
        tenor = 'PT1H'
        AND source = 'RTE' 
        AND country = 'FR'
        AND production_type IN ('SOLAR', 'WIND_ONSHORE', 'WIND_OFFSHORE')
    GROUP BY
        prod_start,
        production_type_modify) AS p
JOIN (
    SELECT 
        * 
    FROM 
        installed_capacities 
    WHERE 
        source = 'ODRE'
        AND country = 'FR'
        AND capacities_type IN ('SOLAR', 'WIND')) AS c
ON p.prod_start >= c.capa_start 
AND p.prod_end <= c.capa_end 
AND p.production_type_modify = c.capacities_type;


-- Capacity factor ODRE - Eco2mix

SELECT 
    *
FROM production_per_type
WHERE tenor = 'PT15M'
AND country = 'FR'
AND production_type IN ('SOLAR', 'WIND')
AND source LIKE 'ODRE_Eco2mix%';


INSERT INTO capacity_factor_per_type (
    cf_start,
    cf_end,
    source,
    country,
    tenor,
    cf_type,
    capacity_factor,
    processing)
SELECT
    p.prod_start AS cf_start,
    p.prod_end AS cf_end,
    p.source AS source,
    p.country AS country,
    p.tenor AS tenor,
    p.production_type AS cf_type,
    p.quantity / c.quantity AS capacity_factor,
    p.processing AS processing
FROM (
    SELECT 
        *
    FROM production_per_type
    WHERE tenor = 'PT15M'
    AND country = 'FR'
    AND production_type IN ('SOLAR', 'WIND')
    AND source LIKE 'ODRE_Eco2mix%'
) AS p
JOIN (
    SELECT 
        * 
    FROM 
        installed_capacities 
    WHERE 
        source = 'ODRE'
        AND country = 'FR'
        AND capacities_type IN ('SOLAR', 'WIND')
) AS c
ON p.prod_start >= c.capa_start
AND p.prod_end <= c.capa_end 
AND p.production_type = c.capacities_type
ORDER BY p.prod_start;