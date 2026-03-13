
-- gold.usp_load_dim_date

CREATE OR ALTER PROCEDURE gold.usp_load_dim_date
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE gold.dim_date;

    DECLARE @start_date DATE = '2015-01-01';
    DECLARE @end_date   DATE = '2030-12-31';

    ;WITH date_series AS (
        SELECT @start_date AS full_date

        UNION ALL

        SELECT DATEADD(DAY, 1, full_date)
        FROM date_series
        WHERE full_date < @end_date
    )
    INSERT INTO gold.dim_date (
        date_key,
        full_date,
        day_number,
        month_number,
        month_name,
        short_month_name,
        quarter_number,
        year_number,
        weekday_number,
        weekday_name,
        short_weekday_name,
        week_of_year,
        is_weekend,
        is_month_start,
        is_month_end,
        is_year_start,
        is_year_end,
        month_start_date,
        month_end_date,
        year_start_date,
        year_end_date,
        year_month,
        year_week
    )
    SELECT
        CONVERT(INT, CONVERT(CHAR(8), full_date, 112)) AS date_key,
        full_date,
        DAY(full_date) AS day_number,
        MONTH(full_date) AS month_number,
        DATENAME(MONTH, full_date) AS month_name,
        LEFT(DATENAME(MONTH, full_date), 3) AS short_month_name,
        DATEPART(QUARTER, full_date) AS quarter_number,
        YEAR(full_date) AS year_number,
        DATEPART(WEEKDAY, full_date) AS weekday_number,
        DATENAME(WEEKDAY, full_date) AS weekday_name,
        LEFT(DATENAME(WEEKDAY, full_date), 3) AS short_weekday_name,
        DATEPART(WEEK, full_date) AS week_of_year,
        CASE
            WHEN DATENAME(WEEKDAY, full_date) IN ('Saturday', 'Sunday') THEN 1
            ELSE 0
        END AS is_weekend,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), MONTH(full_date), 1) THEN 1
            ELSE 0
        END AS is_month_start,
        CASE
            WHEN full_date = EOMONTH(full_date) THEN 1
            ELSE 0
        END AS is_month_end,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), 1, 1) THEN 1
            ELSE 0
        END AS is_year_start,
        CASE
            WHEN full_date = DATEFROMPARTS(YEAR(full_date), 12, 31) THEN 1
            ELSE 0
        END AS is_year_end,
        DATEFROMPARTS(YEAR(full_date), MONTH(full_date), 1) AS month_start_date,
        EOMONTH(full_date) AS month_end_date,
        DATEFROMPARTS(YEAR(full_date), 1, 1) AS year_start_date,
        DATEFROMPARTS(YEAR(full_date), 12, 31) AS year_end_date,
        CONVERT(CHAR(7), full_date, 120) AS year_month,
        CONCAT(YEAR(full_date), '-', RIGHT('0' + CAST(DATEPART(WEEK, full_date) AS VARCHAR(2)), 2)) AS year_week
    FROM date_series
    OPTION (MAXRECURSION 0);
END;
GO
