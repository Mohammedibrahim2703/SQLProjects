select count(1) from covid_data_vac where iso_code = 'AFG' ;


---max death case country wise
SELECT MAX(CASE WHEN total_deaths ~ '^\d+$' THEN total_deaths::int ELSE NULL END), location
FROM covid_data_death
WHERE total_deaths IS NOT NULL
GROUP BY location
ORDER BY 1 DESC;

SELECT
    location,
    date,
    total_vaccinations,
    LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date) AS prev_total_vaccinations,
    CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN total_vaccinations::numeric
        ELSE 0 -- Or any other default value
    END AS total_vaccinations_numeric,
    CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN total_vaccinations::numeric - LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date)
        ELSE 0 -- Or any other default value
    END AS daily_vaccinations,
    ROUND(CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN (((total_vaccinations::numeric - LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date)) / NULLIF(LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date), 0)) * 100)
        ELSE 0 -- Or any other default value
    END, 2) AS percentage_change
FROM
    covid_data_vac
WHERE
    total_vaccinations IS NOT NULL
ORDER BY
    location, date;

SELECT
    location,
    date,
    total_deaths,
    AVG(total_deaths::numeric) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_daily_deaths
FROM
    covid_data_death
WHERE
    total_deaths IS NOT NULL
    AND total_deaths ~ '^\d+(\.\d+)?$'; -- Ensure total_deaths consists of numeric values

SELECT
    location,
    total_vaccinations,
    RANK() OVER (ORDER BY total_vaccinations DESC) AS vaccination_rank
FROM
    covid_data_vac
WHERE
    total_vaccinations IS NOT NULL;

SELECT
    location,
    date,
    total_vaccinations,
    LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date) AS prev_total_vaccinations,
    CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN total_vaccinations::numeric
        ELSE 0 -- Or any other default value
    END AS total_vaccinations_numeric,
    CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN total_vaccinations::numeric - LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date)
        ELSE 0 -- Or any other default value
    END AS daily_vaccinations,
    ROUND(CASE
        WHEN total_vaccinations ~ '^[0-9]+$' THEN (((total_vaccinations::numeric - LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date)) / NULLIF(LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date), 0)) * 100)
        ELSE 0 -- Or any other default value
    END, 2) AS percentage_change
FROM
    covid_data_vac
WHERE
   (total_vaccinations ~ '^[0-9]+$') -- Only consider rows where total_vaccinations consists of digits
ORDER BY
    location, date;
EXPLAIN
SELECT
    v.location,
    v.date AS vac_date,
    v.total_vaccinations,
    d.date AS death_date,
    d.total_deaths
FROM
    covid_data_vac v
INNER JOIN
    covid_data_death d ON v.location = d.location
WHERE
    v.location IS NOT NULL
    AND d.location IS NOT NULL;
	
explain SELECT
    v.location,
    v.date AS vac_date,
    v.total_vaccinations,
    d.date AS death_date,
    d.total_deaths
FROM
    covid_data_vac v
INNER JOIN
    covid_data_death d 
	  ON v.location = d.location
	  -- AND v.date=d.date
WHERE
    v.location IS NOT NULL
    AND d.location IS NOT NULL;
	
--rewrite this using CTE 
WITH vaccinated_locations AS (
    SELECT
        location,
        date AS vac_date,
        total_vaccinations
    FROM
        covid_data_vac
    WHERE
        location IS NOT NULL
),
deaths_with_locations AS (
    SELECT
        location,
        date AS death_date,
        total_deaths
    FROM
        covid_data_death
    WHERE
        location IS NOT NULL
)
SELECT
    v.location,
    v.vac_date,
    v.total_vaccinations,
    d.death_date,
    d.total_deaths
FROM
    vaccinated_locations v
INNER JOIN
    deaths_with_locations d 
    ON v.location = d.location
	 AND v.vac_date=d.death_date;

---vaccination INequities 
WITH vaccinated_locations AS (
    SELECT
        location,
        date AS vac_date,
        total_vaccinations
    FROM
        covid_data_vac
    WHERE
        location IS NOT NULL
),
deaths_with_locations AS (
    SELECT
        location,
        date AS death_date,
        total_deaths
    FROM
        covid_data_death
    WHERE
        location IS NOT NULL
)
SELECT
    v.location,
    v.vac_date,
    v.total_vaccinations,
    d.death_date,
    d.total_deaths
FROM
    vaccinated_locations v
INNER JOIN
    deaths_with_locations d 
    ON v.location = d.location
    AND v.vac_date = d.death_date;
	
	-- Common Table Expression (CTE) to calculate historical vaccination rates for each country
WITH historical_vaccination_data AS (
    SELECT
        location,
        date,
        total_vaccinations,
        LAG(total_vaccinations) OVER (PARTITION BY location ORDER BY date) AS prev_total_vaccinations
    FROM
        covid_data_vac
    WHERE
        location IS NOT NULL
),

-- Common Table Expression (CTE) to calculate vaccination rates (daily changes)
daily_vaccination_rates AS (
    SELECT
        location,
        date,
        total_vaccinations - prev_total_vaccinations AS daily_vaccinations
    FROM
        historical_vaccination_data
    WHERE
        prev_total_vaccinations IS NOT NULL
),

-- Common Table Expression (CTE) to apply time series forecasting techniques

-- Common Table Expression (CTE) to calculate historical vaccination rates for each country
WITH historical_vaccination_data AS (
    SELECT
        location,
        date,
        total_vaccinations,
        LAG(total_vaccinations) OVER (PARTITION BY location ORDER BY date) AS prev_total_vaccinations
    FROM
        covid_data_vac
    WHERE
        location IS NOT NULL
),

-- Common Table Expression (CTE) to calculate vaccination rates (daily changes)
SELECT
    location,
    date,
    total_vaccinations,
    forecasted_vaccinations
FROM (
    SELECT
        location,
        date,
        total_vaccinations,
        LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date) AS prev_total_vaccinations,
        total_vaccinations::numeric - LAG(total_vaccinations::numeric) OVER (PARTITION BY location ORDER BY date) AS daily_vaccinations,
        -- Apply exponential smoothing or ARIMA forecasting techniques here
        -- Example: 
        -- forecasted_vaccinations = exponential_smoothing(total_vaccinations, alpha) 
        -- OR forecasted_vaccinations = ARIMA(total_vaccinations)
        -- You need to use appropriate functions or libraries for the chosen technique
        -- Replace these placeholders with actual forecasted values
        total_vaccinations AS forecasted_vaccinations
    FROM
        covid_data_vac
    WHERE
        location IS NOT NULL
) AS forecasted_vaccination_rates
WHERE
    prev_total_vaccinations IS NOT NULL
ORDER BY
    location, date;

