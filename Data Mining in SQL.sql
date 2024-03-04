/* Query 01: calculate total visits, page views, and transactions for Jan, Feb, and March 2017 (order by month) */

SELECT  
  Format_date("%Y%m", parse_date("%Y%m%d", date)) as month                          -- Chuyển đổi định dạng 'Date' từ YYYYMMDD sang YYYYMM
  
  sum(totals.visits) as visits,                                                     -- Tính tổng lượt thăm trang web, gán metric này tên "Visits"
  
  sum(totals.pageviews) as pageviews,                                               -- Tính tổng lượt bấm lướt các trang trên trang web, gán metric này tên "pageviews"
  
  sum(totals.transactions) as transactions                                          -- Tính tổng lượt giao dịch, gán metric này tên "transactions"
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  
WHERE _table_suffix between '0101' and '0331'                                       -- Giới hạn data trong khoảng từ tháng 1 đến tháng 3 
  
GROUP BY month                                                                      -- Nhóm các giá trị trả lại theo tháng
        
ORDER BY month ASC;                                                                 -- Sắp xếp theo tháng, thứ tự tăng dần (1 -> 3)


/* Query 02: Bounce rate per traffic source in July 2017 */

SELECT  
  trafficSource.source as source,                                                   -- Chọn cột "trafficSource" từ bảng "source"
  
  sum(totals.visits) AS total_visit,                                                -- Tính tổng lượt thăm trang web, gán metric này tên "total_visit"
  
  count(totals.bounces) as total_num_of_bounce,                                     -- Đếm lượt "bounces - visitor chỉ xem trang landing page, không xem trang thứ hai", đặt tên metric này là "total_num_of_bounce"
                                                                                  
  round((100.00 * (count(totals.bounces))/(sum(totals.visits))),2) as Bounce_rate   -- Tính "Bounce_rate - Tỷ lệ thoát" bằng cách lấy "Tổng lượt thoát" chia cho "Tổng số thăm trang", nhân 100, và làm tròn kết quả 2 chữ số sau dấu phẩy.
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  
GROUP BY source                                                                     -- Nhóm các giá trị trả lại theo nguồn truy cập (source)
  
ORDER BY total_visit DESC;                                                          -- Sắp xếp tổng lượt truy cập trang web, thứ tự giảm dần.

/* Query 3: Revenue by traffic source by week, by month in June 2017 */

with month_revenue as (                                                             -- Khởi tạo CTE, tên là "month_revenue"
  SELECT 
  'month' as time_type,
  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as time,
  
  trafficSource.source as source,
  
  (SUM(productRevenue)/1000000) AS Revenue                                          -- Tính tổng doanh thu của sản phẩm (chia bớt 6 số 0 để gọn số), gán giá trị với tên "Revenue"
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  
UNNEST (hits) hits,                                                                 -- Dùng "Unnest" để tiếp cận Array cần lấy giá trị. Ở đây muốn tiếp cận cột "productRevenue" thì phải "Unnest" bảng "hits" và bảng "hits.product".
UNNEST (hits.product) product
  
WHERE productRevenue is not null                                                    -- Đặt điều kiện "productRevenue" is not null
  
group by source,time),                                                              -- Nhóm kết quả bằng cột "time" trong bảng "source"


week_revenue as (                                                                   -- Khởi tạo CTE, tên là "week_revenue"                                                 
  SELECT 
  'week' as time_type,
  
  FORMAT_DATE('%Y%W', DATETIME (PARSE_DATE('%Y%m%d', date))) as time,                
  
  trafficSource.source as source,
  
  (SUM(productRevenue)/1000000) AS Revenue                                          -- Tổng Doanh thu sản phẩm (chia 6 số không) 
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
WHERE productRevenue is not null
  
GROUP BY source,time)

  
SELECT *
FROM month_revenue
  
UNION ALL                                                                            -- Ghép 2 CTEs lại tạo thành 1 bảng kết quả chứa "Doanh thu theo traffic" tính theo tuần và tháng.
  
SELECT * 
FROM week_revenue;

/* Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, and July 2017. */

WITH P1 AS (                                                                         -- Khởi tạo cte p1 chứa trung bình lượt xem trang và có mua hàng. 
SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  
  SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) as avg_pageviews_purchase      -- Tính AVG pageView/mỗi Lần vào website (= tổng pageView / Lần vào website)
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
WHERE _table_suffix between '0601' and '0731' and                                    -- Sử dụng Wildcard "_table_suffix" để rút gọn câu code (tượng trưng cho phần ký tự có thể thay đổi trong các hàng dữ liệu).
  
      totals.transactions >=1 and
  
      productRevenue is not null
  
GROUP BY month),

p2 as (                                                                              -- Khởi tạo cte p2 chứa trung bình lượt xem trang nhưng không mua hàng.
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
LEFT JOIN p2                                                                         -- Sử dụng Left join để join 2 cte p1 và p2 lại với nhau.
USING (month);

/* Query 05: Average number of transactions per user that purchased in July 2017 */

SELECT  
  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  
  (SUM(totals.transactions)) / COUNT(distinct fullVisitorId) as Avg_total_transactions_per_user        -- Tính trung bình số giao dịch trên mỗi user
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
WHERE totals.transactions >=1 and
  
      productRevenue is not null
  
GROUP BY month;

/* Query 06: Average amount of money spent per session. Only include purchaser data in July 2017 */

SELECT  
  FORMAT_DATE('%Y%m', DATETIME (PARSE_DATE('%Y%m%d', date))) as month,
  
  ROUND(
    (((SUM(productRevenue)) / COUNT(totals.visits))/1000000),2) as Avg_Revenue_per_user                -- Tính trung bình Doanh thu trên mỗi user
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
WHERE totals.transactions >=1 and
  
      productRevenue is not null
  
GROUP BY month;

/* Query 07: Other products purchased by customers who purchased the product "YouTube Men's Vintage Henley" in July 2017. The output should show the product name and the quantity ordered. */

with user1 as(
SELECT 
  
      fullVisitorId
  
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
WHERE v2ProductName = "YouTube Men's Vintage Henley" and                            -- Sau khi Unnest và reach được tới bảng "product", lọc ra những giao dịch hợp lệ với sản phẩm là "YouTube Men's Vintage Henley".
  
      productRevenue is not null                                  
  
GROUP BY fullVisitorId)                                                             -- Đến bước này ta sẽ có cte chứa ID của những user mua sp tên "YouTube Men's Vintage Henley".                                                               

SELECT  
  
      v2ProductName AS other_purchased_products,
  
      SUM(productQuantity) AS quantity
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` as p2,
  
UNNEST (hits) hits,
UNNEST (hits.product) product
  
INNER JOIN user1 ON user1.fullVisitorId = p2.fullVisitorId                           -- Inner join cte user1 với p2 để tạo ra bảng chứa tên và số lương các sản phẩm được mua khi người mua đã mua sản phẩm "YouTube Men's Vintage Henley".
  
WHERE productRevenue is not null
  
GROUP BY v2ProductName;

/* Query 08: Calculate cohort map from product view to add to cart to purchase in Jan, Feb, and March 2017. 

hits.eCommerceAction.action_type = '2' is view product page; 
hits.eCommerceAction.action_type = '3' is add to cart; 
hits.eCommerceAction.action_type = '6' is purchase

*/

WITH product_data as(
SELECT
  
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
  
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,                                      -- Dùng "count(CASE WHEN)" để phân loại và đếm số lượng các action_type:
                                                                                                                                                -- "product_view, add_to_cart and purchase"
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,                                    
  
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
  
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  
UNNEST(hits) as hits,
UNNEST (hits.product) as product
  
WHERE _table_suffix between '20170101' and '20170331'                                                                                           -- Lọc dữ liệu từ tháng 1/2017 đến hết tháng 3/2017 và 
                                                                                                                                                -- filter thêm action_type 1 lần nữa để chắc chắn.
and eCommerceAction.action_type in ('2','3','6')                                      
  
GROUP BY month
ORDER BY month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,                                                                       -- Tính tỷ lệ add to cart trên số lần xem sản phẩm 
  
    round(num_purchase/num_product_view * 100, 2) as purchase_rate                                                                              -- và tỷ lệ mua hàng trên số lần xem sản phẩm 
  
from product_data;
