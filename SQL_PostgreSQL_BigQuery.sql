-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL
select 
    format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    sum(totals.visits) as visits, 
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    safe_divide(sum(totals.totalTransactionRevenue),1000000) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by month
order by month;


-- Query 02: Bounce rate per traffic source in July 2017

#standardSQL
select 
    trafficSource.source,
    sum(totals.visits) as total_visits,
    sum(totals.bounces) as total_no_of_bounces,
    round(safe_divide(sum(totals.bounces),sum(totals.visits))*100,8) as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc
limit 4;

-- Query 3: Revenue by traffic source by week, by month in June 2017

#standardSQL
with revenue_month as (
    select 
        "Month" as time_type,
        format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as time,
        trafficSource.source,
        safe_divide(sum(totals.totalTransactionRevenue),1000000) as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by trafficSource.source, time_type, time
),

revenue_week as (
    select 
        "Week" as time_type,
        format_date("%Y%W",PARSE_DATE('%Y%m%d',date)) as time,
        trafficSource.source,
        safe_divide(sum(totals.totalTransactionRevenue),1000000) as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by trafficSource.source, time_type, time
)

select *
from revenue_month
union all 
select *
from revenue_week
order by revenue desc
limit 4;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

#standardSQL
with sub1 as (
    select 
        format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(totals.pageviews) as total1,
        fullVisitorId
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170601' and '20170731'
    and totals.transactions >=1
    group by fullVisitorId, month
),
sub2 as (
    select 
        format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(totals.pageviews) as total2,
        fullVisitorId
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170601' and '20170731'
    and totals.transactions is null
    group by fullVisitorId, month
)
select
    sub1.month,
    round(avg(total1),8) as avg_pageviews_purchase,
    round(avg(total2),9) as avg_pageviews_non_purchase
from sub1
join sub2 on sub1.month=sub2.month
group by month
order by month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017

#standardSQL
with sub as (
select
    format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    sum(totals.transactions) as total,
    fullVisitorId
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >=1
group by fullVisitorId, month
)
select
    month,
    round(avg(total),9) as Avg_total_transactions_per_user
from sub
group by month;


-- Query 06: Average amount of money spent per session

#standardSQL
with sub1 as (
    select 
        format_date("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(totals.totaltransactionRevenue) as total_revenue,
        visitId
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    where totals.transactions is not null
    group by visitId, month
)
select 
    month,
    round(avg(total_revenue),6) as avg_revenue_by_user_per_visit
from sub1
group by month;


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)

#standardSQL
with sub1 as (
    select fullVisitorId, v2ProductName, 
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    cross join Unnest(hits)
    cross join unnest(product)
    where v2ProductName = "YouTube Men's Vintage Henley" and productRevenue is not null
)
select 
    v2ProductName as other_purchased_products,
    sum(productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
cross join Unnest(hits)
cross join unnest(product)
where fullVisitorId in (select fullVisitorId from sub1)
and productRevenue is not null
and v2ProductName != "YouTube Men's Vintage Henley"
group by v2ProductName
order by quantity desc;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

#standardSQL
with sub1 as (
 select 
     format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
     COUNT(v2ProductName) as num_product_view
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
where ecommerceaction.action_type = '2'
AND (isImpression IS NULL OR isImpression = FALSE)
group by month
),

sub2 as (
    select 
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        COUNT(v2ProductName) as num_addtocart
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
where ecommerceaction.action_type = '3'
group by month
),

sub3 as (
    select 
        format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
        COUNT(v2ProductName) as num_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
CROSS JOIN UNNEST(hits)
CROSS JOIN UNNEST(product)
where ecommerceaction.action_type = '6'
group by month
)

SELECT
    sub1.month,
    num_product_view,
    num_addtocart,
    num_purchase,
    round(Safe_divide(num_addtocart,num_product_view)*100,2) as add_to_cart_rate,
    round(Safe_divide(num_purchase,num_product_view)*100,2) as purchase_rate
from sub1
join sub2 using(month) 
join sub3 using(month) 
order by month
limit 3;
