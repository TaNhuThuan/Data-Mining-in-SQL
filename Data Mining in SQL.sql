--Lưu ý chung: với Bigquery thì mình có thể group by, order by 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Mình k nên xử lý date bằng những hàm đc dùng để xử lý chuỗi như left, substring, concat
--vì lúc này data của mình vẫn ở dạng string, chứ k phải dạng date, khi xuất ra excel hay gg sheet thì phải xử lý thêm 1 bước nữa


-- Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)

SELECT  
  LEFT(date,6) as month,  --> format_date("%Y%m", parse_date("%Y%m%d", date)) as month
  sum(totals.visits) visits,
  sum(totals.pageviews) pageviews,
  sum(totals.transactions) transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0101' and '0331'
GROUP BY month
ORDER BY month ASC;
--correct

-- Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)

SELECT  
  trafficSource.source as source,
  sum(totals.visits) AS total_visit,
  count(totals.bounces) as total_num_of_bounce,
  round((100.00 * (count(totals.bounces))/(sum(totals.visits))),2) as Bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source 
ORDER BY total_visit DESC;
--correct

-- Query 3: Revenue by traffic source by week, by month in June 2017

with month_revenue as (
  SELECT 
  'month' as time_type,
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as time,
  trafficSource.source as source,
  (SUM(productRevenue)/1000000) AS Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE productRevenue is not null
group by source,time),

week_revenue as (
  SELECT 
  'week' as time_type,
  FORMAT_DATE('%Y%W', DATETIME (PARSE_DATE('%Y%m%d', date))) as time,
  trafficSource.source as source,
  (SUM(productRevenue)/1000000) AS Revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE productRevenue is not null
GROUP BY source,time)

SELECT *
FROM month_revenue
UNION ALL
SELECT * 
FROM week_revenue;
--correct

-- Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

WITH P1 AS (
SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix between '0601' and '0731' and
      totals.transactions >=1 and
      productRevenue is not null
GROUP BY month),

p2 as (
SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) as avg_pageviews_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix between '0601' and '0731' and
      totals.transactions is null and
      productRevenue is null
GROUP BY month)

SELECT *
FROM p1 
INNER JOIN p2   --> left join/ full join
USING (month);

--câu 4 này lưu ý là mình nên dùng left join hoặc full join, bởi vì trong câu này, phạm vi chỉ từ tháng 6-7, nên chắc chắc sẽ có pur và nonpur của cả 2 tháng
--mình inner join thì vô tình nó sẽ ra đúng. nhưng nếu đề bài là 1 khoảng thời gian dài hơn, 2-3 năm chẳng hạn, nó cũng tháng chỉ có nonpur mà k có pur
--thì khi đó inner join nó sẽ làm mình bị mất data, thay vì hiện số của nonpur và pur thì nó để trống

-- Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  (SUM(totals.transactions)) / COUNT(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE totals.transactions >=1 and
      productRevenue is not null
GROUP BY month;
--correct

-- Query 06: Average amount of money spent per session. Only include purchaser data in July 2017

SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  ROUND(
    (((SUM(productRevenue)) / COUNT(totals.visits))/1000000),2) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE totals.transactions >=1 and
      productRevenue is not null
GROUP BY month;
--correct

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

with user1 as(
SELECT 
      fullVisitorId
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE v2ProductName = "YouTube Men's Vintage Henley" and
      productRevenue is not null
GROUP BY fullVisitorId)

SELECT  
      v2ProductName AS other_purchased_products,
      SUM(productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as p2,
UNNEST (hits) hits,
UNNEST (hits.product) product
LEFT JOIN user1 ON user1.fullVisitorId = p2.fullVisitorId   --> inner join
WHERE productRevenue is not null
GROUP BY v2ProductName;
--> ở phần này thì mình nên dùng inner join, bởi vì mình chỉ muốn lấy tập giao của fullvisitorId thôi 

--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017.

with view1 as(
SELECT
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  COUNT(eCommerceAction.action_type) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits
WHERE _table_suffix between '0101' and '0331'
      and eCommerceAction.action_type = '2'
GROUP BY month),

addtocart as(
SELECT
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  COUNT(eCommerceAction.action_type) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits
WHERE _table_suffix between '0101' and '0331'
      and eCommerceAction.action_type = '3'
GROUP BY month),

purchase as(
SELECT
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  COUNT(eCommerceAction.action_type) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE _table_suffix between '0101' and '0331'
      and eCommerceAction.action_type = '6'
      and productRevenue is not null
GROUP BY month)

SELECT 
  month,
  num_product_view,
  num_addtocart,
  num_purchase,
  ROUND((100 *(num_addtocart/num_product_view)),2) as add_to_cart_rate,
  ROUND((100 *(num_purchase/num_product_view)),2) as purchase_rate
FROM view1
INNER JOIN addtocart USING (month)
INNER JOIN purchase USING (month)
GROUP BY month,num_product_view,num_addtocart,num_purchase
ORDER BY month;

--bài yêu cầu tính số sản phầm, mình nên count productName hay productSKU thì sẽ hợp lý hơn là count action_type
--k nên xài inner join, nếu table1 có 10 record,table2 có 5 record,table3 có 1 record, thì sau khi inner join, output chỉ ra 1 record

--Cách 1:dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;

--bài này k nên inner join, vì nếu như bảng purchase k có data thì sẽ k mapping đc vs bảng productview, từ đó kết quả sẽ k có luôn, mình nên dùng left join

--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;


                                                          ---very good---