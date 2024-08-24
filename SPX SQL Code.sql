
/*
	Title: RE: Is it worth investing at all-time highs?
*/

/*
	Abbreviation:
	
	spx: s&p 500 index
	ath: all-time highs
	ctd: close to date
	avg: average
	yr: year
*/

/*
	Create schema to store table.
*/

CREATE SCHEMA spx_analysis
;

/*
	Create table to import S&P 500 Index (SPX) Cummulative Total Return data, set data types.

	Reminder for import compatibility: in the .csv file we get from Bloomberg for the project, we changed the trade date format directly in the .csv to YYYY-MM-DD.
*/

DROP TABLE spx_analysis.spx_base; -- when want to update
CREATE TABLE spx_analysis.spx_base (
	trade_date DATE,
	close_price DECIMAL(9,2)
)
;

/*
	18783 rows imported. (1950-01-01 to 2024-08-23)
*/

/*
	TRANSFORMATION START HERE:
*/

/*
	Add principal amount (100). [in order to calculate return later, avoid dividing by 0]
*/

DROP TABLE IF EXISTS spx_principal;
CREATE TEMP TABLE spx_principal AS(
SELECT
	t.trade_date,
	(t.close_price + 100) AS close_price
FROM spx_analysis.spx_base t
)
;

/*
	Display the highest closing price to date for each date.
*/

DROP TABLE IF EXISTS spx_ath_ctd;
CREATE TEMP TABLE spx_ath_ctd AS(
SELECT
	t.trade_date,
	t.close_price,
	MAX(t.close_price) OVER(ORDER BY t.trade_date 
							ROWS BETWEEN UNBOUNDED PRECEDING
							AND CURRENT ROW) AS ath_ctd
FROM spx_principal t
GROUP BY
	t.trade_date,
	t.close_price
ORDER BY t.trade_date
)
;

/*
	Display the prior day's highest closing price.
*/

DROP TABLE IF EXISTS spx_prior_ath_ctd;
CREATE TEMPORARY TABLE spx_prior_ath_ctd AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	LAG(ath_ctd) OVER(ORDER BY t.trade_date) AS prior_ath_ctd
FROM spx_ath_ctd t
ORDER BY t.trade_date
)
;

/*
	Determine if the close price for date is an all-time high by marking Y/N comparing previous 2 columns made.
*/

DROP TABLE IF EXISTS spx_new_ath_marker;
CREATE TEMPORARY TABLE spx_new_ath_marker AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	CASE
	-- CASE logic: if the current ath price is the same as yesterday's then it is not a new ath.
		WHEN t.ath_ctd = t.prior_ath_ctd THEN 'N'
		ELSE 'Y'
	END AS new_ath_marker
FROM spx_prior_ath_ctd t
ORDER BY t.trade_date
)
;

/*
	Display the future sale date (in 1,3,5 year) of each purchase date.

	Then, mark their their unique position for upcoming self LEFT JOIN logic calculation.
*/


DROP TABLE IF EXISTS spx_future_trade_date;
CREATE TEMPORARY TABLE spx_future_trade_date AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	CAST((t.trade_date + INTERVAL '1 YEAR') AS DATE) AS in_1yr_trade_date,
	CAST((t.trade_date + INTERVAL '3 YEAR') AS DATE) AS in_3yr_trade_date,
	CAST((t.trade_date + INTERVAL '5 YEAR') AS DATE) AS in_5yr_trade_date,
	ROW_NUMBER() OVER(ORDER BY t.trade_date) AS row_marker
FROM spx_new_ath_marker t
ORDER BY t.trade_date
)
;

/*
	Display row number of future sale date (in 1,3,5 year) through progressive table building upon itself.

	Split the operation into 3 to decrease processing time.

	LEFT JOIN logic: if there is no row number for the exact future sale date, grab 10 days after row numbers and select the lowest value as next best row number for sale date. tldr: if you can't sell on the exact future date, choose the next closest trade date to sell.
*/

-- 1 year sale date row marker.
DROP TABLE IF EXISTS spx_in_1yr_trade_row;
CREATE TEMPORARY TABLE spx_in_1yr_trade_row AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	MIN(i1.row_marker) AS in_1yr_row_marker
FROM spx_future_trade_date t
LEFT JOIN spx_future_trade_date i1
	ON t.in_1yr_trade_date <= i1.trade_date
	AND t.in_1yr_trade_date >= i1.trade_date - INTERVAL '10 DAYS'
GROUP BY
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker
ORDER BY t.trade_date
)
;

-- 3 year sale date row marker.
DROP TABLE IF EXISTS spx_in_3yr_trade_row;
CREATE TEMPORARY TABLE spx_in_3yr_trade_row AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	t.in_1yr_row_marker,
	MIN(i3.row_marker) AS in_3yr_row_marker
FROM spx_in_1yr_trade_row t
LEFT JOIN spx_in_1yr_trade_row i3
	ON t.in_3yr_trade_date <= i3.trade_date
	AND t.in_3yr_trade_date >= i3.trade_date - INTERVAL '10 DAYS'
GROUP BY
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	t.in_1yr_row_marker
ORDER BY t.trade_date
)
;

-- 5 year sale date row marker.
DROP TABLE IF EXISTS spx_in_5yr_trade_row;
CREATE TEMPORARY TABLE spx_in_5yr_trade_row AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	t.in_1yr_row_marker,
	t.in_3yr_row_marker,
	MIN(i5.row_marker) AS in_5yr_row_marker
FROM spx_in_3yr_trade_row t
LEFT JOIN spx_in_3yr_trade_row i5
	ON t.in_5yr_trade_date <= i5.trade_date
	AND t.in_5yr_trade_date >= i5.trade_date - INTERVAL '10 DAYS'
GROUP BY
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	t.in_1yr_row_marker,
	t.in_3yr_row_marker
ORDER BY t.trade_date
)
;

/*
	Calculate the dollar return and percent return of our purchase over time for each date.

	Included a WHERE statement if we want to do different time parameter.

	LEFT JOIN logic: align the future row marker columns (1,3,5) with the initial row marker for each column to calculate performance.
*/

DROP TABLE IF EXISTS spx_trade_return;
CREATE TEMPORARY TABLE spx_trade_return AS(
SELECT
	t.trade_date,
	t.close_price,
	t.ath_ctd,
	t.prior_ath_ctd,
	t.new_ath_marker,
	t.in_1yr_trade_date,
	t.in_3yr_trade_date,
	t.in_5yr_trade_date,
	t.row_marker,
	t.in_1yr_row_marker,
	t.in_3yr_row_marker,
	t.in_5yr_row_marker,

	-- Dollar difference in 1yr.
	(i1.close_price - t.close_price) 
	AS in_1yr_dollar_return,

	-- Return percent in 1yr.

	ROUND(((i1.close_price - t.close_price)/t.close_price),4) 
	AS in_1yr_percent_return,

	-- Dollar difference in 3yr.
	(i3.close_price - t.close_price) 
	AS in_3yr_dollar_return,

	-- Return percent in 3yr.
	ROUND(((i3.close_price - t.close_price)/t.close_price),4) 
	AS in_3yr_percent_return,

	-- Dollar difference in 5yr.
	(i5.close_price - t.close_price) 
	AS in_5yr_dollar_return,

	-- Return percent in 5yr.
	ROUND(((i5.close_price - t.close_price)/t.close_price),4) 
	AS in_5yr_percent_return

FROM spx_in_5yr_trade_row t

	LEFT JOIN 
	spx_in_5yr_trade_row i1	 
	ON t.in_1yr_row_marker = i1.row_marker

	LEFT JOIN
	spx_in_5yr_trade_row i3
	ON t.in_3yr_row_marker = i3.row_marker

	LEFT JOIN
	spx_in_5yr_trade_row i5
	ON t.in_5yr_row_marker = i5.row_marker
WHERE
		TRUE
--	AND t.trade_date >= '1988-01-01'
)
;

/*
	ANALYSIS START HERE:
*/

/*
	SUMMARIZE: Calculate the average percent return of any day vs. all-time high purchase timing.
*/

DROP TABLE IF EXISTS spx_any_day_vs_ath_avg_return;
CREATE TEMPORARY TABLE spx_any_day_vs_ath_avg_return AS(

-- any day timing average percent return
SELECT
	'any_day' AS timing,
	ROUND(AVG(t.in_1yr_percent_return),4) AS in_1yr_avg_return,
	ROUND(AVG(t.in_3yr_percent_return),4) AS in_3yr_avg_return,
	ROUND(AVG(t.in_5yr_percent_return),4) AS in_5yr_avg_return
FROM spx_trade_return t

UNION ALL 

-- all-time high timing average percent return
SELECT
	'all_time_high' AS timing,
	ROUND(AVG(t.in_1yr_percent_return),4) AS in_1yr_avg_return,
	ROUND(AVG(t.in_3yr_percent_return),4) AS in_3yr_avg_return,
	ROUND(AVG(t.in_5yr_percent_return),4) AS in_5yr_avg_return
FROM spx_trade_return t
WHERE new_ath_marker = 'Y'
)
;

/*
	Display the median percent return of any day vs. all-time high purchase timing.

	Break the process down into 3 steps for each year after purchase.

	Context: While Average return considers all data points and are good for models. Stock data are very much influenced by extreme events. Median return would give us a more realistic performance expectation for this analysis.

	Window Function logic: sort (1,3,5)yr percent return in order. Then get the maximum value below or equal to the 50th percentile for median.
*/

-- 1 year median percent return.
DROP TABLE IF EXISTS spx_in_1yr_median_return;
CREATE TEMPORARY TABLE spx_in_1yr_median_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_1yr_percent_return) AS in_1yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_1yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_1yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return
	) AS p
WHERE
	TRUE
AND p.percentile <= .5

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_1yr_percent_return) AS in_1yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_1yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE 
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_1yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return
	) AS p
WHERE
	TRUE
AND p.percentile <= .5
)
;

-- 3 year median percent return.
DROP TABLE IF EXISTS spx_in_3yr_median_return;
CREATE TEMPORARY TABLE spx_in_3yr_median_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_3yr_percent_return) AS in_3yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_3yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_3yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= .5

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_3yr_percent_return) AS in_3yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_3yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE 
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_3yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= .5
)
;

-- 5 year median percent return.
DROP TABLE IF EXISTS spx_in_5yr_median_return;
CREATE TEMPORARY TABLE spx_in_5yr_median_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_5yr_percent_return) AS in_5yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_5yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_5yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= .5

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_5yr_percent_return) AS in_5yr_median_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_5yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_5yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= .5
)
;

-- SUMMARIZE: any day vs. all time high timing median percent return.
DROP TABLE IF EXISTS spx_any_day_vs_ath_median_return;
CREATE TEMPORARY TABLE spx_any_day_vs_ath_median_return AS(
SELECT
	i1.timing,
	i1.in_1yr_median_return,
	i3.in_3yr_median_return,
	i5.in_5yr_median_return

FROM spx_in_1yr_median_return i1
	
	JOIN
	spx_in_3yr_median_return i3
	ON i1.timing = i3.timing

	JOIN
	spx_in_5yr_median_return i5
	ON i1.timing = i5.timing
-- this ORDER BY is to get it in similar summary order as average percent return summary
ORDER BY i1.timing DESC
)
;

/*
	Current percentile: conservative 25th percentile.

	Other than median, created a return percentile reference point to for custom return expectation.
*/

DROP TABLE IF EXISTS spx_return_percentile;
CREATE TEMPORARY TABLE spx_return_percentile AS(
SELECT 
	0.25 AS return_percentile
)
;

/*
	Display (x)th percentile percent return.
*/

-- 1 year percentile percent return.
DROP TABLE IF EXISTS spx_in_1yr_percentile_return;
CREATE TEMPORARY TABLE spx_in_1yr_percentile_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_1yr_percent_return) AS in_1yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_1yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_1yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_1yr_percent_return) AS in_1yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_1yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE 
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_1yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_1yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)
)
;

-- 3 year percentile percent return.
DROP TABLE IF EXISTS spx_in_3yr_percentile_return;
CREATE TEMPORARY TABLE spx_in_3yr_percentile_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_3yr_percent_return) AS in_3yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_3yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_3yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_3yr_percent_return) AS in_3yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_3yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE 
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_3yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_3yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)
)
;


-- 5 year percentile percent return.
DROP TABLE IF EXISTS spx_in_5yr_percentile_return;
CREATE TEMPORARY TABLE spx_in_5yr_percentile_return AS(
SELECT
	'any_day' AS timing,
	MAX(p.in_5yr_percent_return) AS in_5yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_5yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.in_5yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)

UNION ALL

SELECT
	'all_time_high' AS timing,
	MAX(p.in_5yr_percent_return) AS in_5yr_percentile_return
FROM(
	SELECT
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return,
		PERCENT_RANK() OVER(ORDER BY t.in_5yr_percent_return) AS percentile
	FROM spx_trade_return t
	WHERE
		TRUE
	AND	t.new_ath_marker = 'Y'
	AND t.in_5yr_percent_return IS NOT NULL
	GROUP BY 
		t.trade_date,
		t.new_ath_marker,
		t.in_5yr_percent_return
	) AS p
WHERE
	  TRUE
  AND p.percentile <= (SELECT return_percentile
  					   FROM spx_return_percentile)
)
;

-- SUMMARIZE: any day vs. all time high timing percentile percent return.
DROP TABLE IF EXISTS spx_any_day_vs_ath_percentile_return;
CREATE TEMPORARY TABLE spx_any_day_vs_ath_percentile_return AS(
SELECT
	i1.timing,
	i1.in_1yr_percentile_return,
	i3.in_3yr_percentile_return,
	i5.in_5yr_percentile_return

FROM spx_in_1yr_percentile_return i1
	
	JOIN
	spx_in_3yr_percentile_return i3
	ON i1.timing = i3.timing

	JOIN
	spx_in_5yr_percentile_return i5
	ON i1.timing = i5.timing
ORDER BY i1.timing DESC
)
;

/*
	Creating table for Tableau dashboard analysis.
*/

/*
	Any day purchasing timing.
*/

DROP TABLE IF EXISTS any_day_timing_return;
CREATE TEMPORARY TABLE any_day_timing_return AS(

-- any day timing average percent return
SELECT
	t.trade_date,
	t.new_ath_marker,
	'1 year' AS period,
	ROUND(t.in_1yr_percent_return,4) AS return
FROM spx_trade_return t

UNION ALL 

SELECT
	t.trade_date,
	t.new_ath_marker,
	'3 year' AS period,
	ROUND(t.in_3yr_percent_return,4) AS return
FROM spx_trade_return t

UNION ALL

SELECT
	t.trade_date,
	t.new_ath_marker,
	'5 year' AS period,
	ROUND(t.in_5yr_percent_return,4) AS return
FROM spx_trade_return t
)
;

/* 
Other
*/


