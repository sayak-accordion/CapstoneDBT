WITH CustomerFirstCampaign AS (
    SELECT
        fs.customerkey,
        dmc.campaignkey,
        ROW_NUMBER()OVER(
            PARTITION BY fs.customerkey
            ORDER BY dd.fulldate ASC
        ) AS rn
    FROM
        gold.fact_sales fs JOIN gold.dim_marketingcampaign dmc
        ON dmc.campaignkey=fs.campaignkey
        JOIN gold.dim_date dd
        ON fs.datekey=dd.datekey
    WHERE 
        dd.fulldate BETWEEN dmc.startdate AND dmc.enddate
),
CampaignNewCustomer AS (
    SELECT 
        campaignkey,
        COUNT(1) AS NewCustomers
    FROM
        CustomerFirstCampaign
    GROUP BY
        campaignkey
),
CampaignTotalSales AS (
    SELECT
        dmc.campaignkey,
        SUM(fs.totalsalesamount) AS TotalSalesInfluence,
    FROM
        gold.fact_sales fs JOIN gold.dim_marketingcampaign dmc
        ON dmc.campaignkey=fs.campaignkey
        JOIN gold.dim_date dd
        ON fs.datekey=dd.datekey
    WHERE 
        dd.fulldate BETWEEN dmc.startdate AND dmc.enddate
    GROUP BY dmc.campaignkey
),
BuildRepeatPurchaseFlag AS (
    SELECT 
        fs.customerKey,
        fs.saleskey,
        fs.productkey,
        fs.campaignkey,
        ROW_NUMBER()OVER(
            PARTITION BY customerkey, productkey
            ORDER BY dd.fulldate ASC
        ) AS rn
    FROM
        gold.fact_sales fs JOIN gold.dim_date dd
        ON fs.datekey = dd.datekey
),
RepeatPurchase AS (
    SELECT
        campaignkey,
        COUNT(DISTINCT
            CASE
                WHEN rn>1 THEN customerkey
            END
        ) AS RepeatPurchase,
        COUNT(DISTINCT 
            CASE
                WHEN rn=1 THEN customerkey
            END
        ) AS FirstPurchase,
        CONCAT(
            CAST(RepeatPurchase*100/FirstPurchase AS DECIMAL(10,2))
            ,'%'
        )AS RepeatPurchaseRate
    FROM
        BuildRepeatPurchaseFlag
    GROUP BY
        campaignkey
)
SELECT 
    dmc.campaignKey,
    cts.TotalSalesInfluence,
    nc.NewCustomers,
    rp.RepeatPurchaseRate,
    CAST(
        (cts.TotalSalesInfluence - dmc.budget)/(dmc.budget*100)
        AS DECIMAL(10,2)
    ) AS ROI
FROM
    gold.dim_marketingcampaign dmc JOIN CAMPAIGNTOTALSALES cts
    ON dmc.campaignkey = cts.campaignkey
    JOIN CAMPAIGNNEWCUSTOMER nc
    ON dmc.campaignkey = nc.campaignKey
    JOIN REPEATPURCHASE rp
    ON dmc.campaignkey = rp.campaignKey