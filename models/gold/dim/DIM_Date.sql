SELECT 
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS DateKey,
    d::DATE AS FullDate,
    YEAR(d) AS Year,
    QUARTER(d) AS Quarter,
    MONTH(d) AS Month,
    WEEKOFYEAR(d) AS WeekOfYear,
    DAYOFWEEK(d) AS DayOfWeek,
    CASE 
        WHEN TO_CHAR(d, 'MM-DD') IN ('01-01', '12-25') THEN TRUE
        ELSE FALSE
    END AS HolidayFlagUS,
    CASE 
        WHEN TO_CHAR(d, 'MM-DD') BETWEEN '03-20' AND '06-20' THEN 'Spring'
        WHEN TO_CHAR(d, 'MM-DD') BETWEEN '06-21' AND '09-21' THEN 'Summer'
        WHEN TO_CHAR(d, 'MM-DD') BETWEEN '09-22' AND '12-20' THEN 'Fall'
        ELSE 'Winter'
    END AS Season
FROM (
    SELECT DATEADD(DAY, seq4(), '2020-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 3651))  -- 0 to 3650 days
) t
