SELECT
    UUID_STRING(
        'efc3a943-a363-4dc4-847e-06b8b0cfddc5',
        CONCAT(os.ORDER_ID,dp.productid)
    ) AS SalesKey,
    os.ORDER_ID AS OrderID, 
    dc.CUSTOMERKEY,
    dp.PRODUCTKEY,
    ds.STOREKEY,
    TO_NUMBER(TO_CHAR(ORDER_DATE, 'YYYYMMDD')) AS DateKey,
    de.EMPLOYEEKEY,
    dmc.CAMPAIGNKEY,
    ops.quantity AS QuantitySold,
    ops.unit_price AS UnitPrice,
    (QuantitySold * UnitPrice) AS TotalSalesAmount,
    (QuantitySold * dp.costprice) AS CostAmount,
    (TotalSalesAmount-CostAmount) AS ProfitAmount,
    ops.DISCOUNT_AMOUNT,
    os.SHIPPING_COST AS ShippingCost,
    ds.REGION,
    os.order_source AS Channel,
    dc.customersegment AS CustomerSegmentImpact
FROM
    {{source('silver','orders_slvr')}} os JOIN {{source('silver','order_product_slvr')}} ops
    ON os.order_id = ops.order_id
    JOIN {{source('gold','DIM_Customer')}} dc
    ON os.customer_id = dc.customerid
    JOIN {{source('gold','DIM_Product')}} dp
    ON ops.product_id = dp.productid
    JOIN {{source('gold','DIM_Store')}} ds
    ON os.store_id = ds.storeid
    JOIN {{source('gold','DIM_Supplier')}} dsp
    ON dp.supplierid=dsp.supplierid
    JOIN {{source('gold','DIM_Employee')}} de
    ON os.employee_id= de.employeeid
    JOIN {{source('gold','DIM_MarketingCampaign')}} dmc
    ON os.campaign_id = dmc.campaignid

