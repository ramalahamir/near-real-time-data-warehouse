use DW_project_dummy;

-- query 01: top 5 products by revenue, split by weekdays and weekends,
-- with monthly breakdowns for a year.
-- assuming the year 2020
select p.product_id, p.product_category, t.month,
case when t.day_of_week in ('Saturday','Sunday') then 'weekend' else 'weekday' end as day_type,
sum(f.purchase_amount) as total_revenue
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
where t.year = 2020
group by p.product_id, p.product_category, t.month, day_type
order by total_revenue desc
limit 5;

-- query 02: Analyzes total purchase amounts by gender and age, detailed by city category.
select c.gender, c.age, c.city_category,
sum(f.purchase_amount) as total_purchase
from salefact f
join customer_dim c on f.customer_id = c.customer_id
group by c.gender, c.age, c.city_category;

-- query 03: total sales for each product category based on customer occupation
select p.product_category, c.occupation, sum(f.purchase_amount) as total_sales
from salefact f
join product_dim p on f.product_id = p.product_id
join customer_dim c on f.customer_id = c.customer_id
group by p.product_category, c.occupation;

-- query 04: Tracking purchase amounts by gender and age 
-- across quarterly periods for the current year
-- assuming the current year is 2020 
-- because that is the latest year present in the record
select c.gender, c.age, t.quarter,
sum(f.purchase_amount) as total_purchase
from salefact f
join customer_dim c on f.customer_id = c.customer_id
join time_dim t on f.date_id = t.date_id
where t.year = 2020
group by c.gender, c.age, t.quarter;

-- query 05: Highlighting the top 5 occupations driving sales within each product category
select p.product_category, c.occupation, sum(f.purchase_amount) as total_sales
from salefact f
join product_dim p on f.product_id = p.product_id
join customer_dim c on f.customer_id = c.customer_id
group by p.product_category, c.occupation
order by p.product_category, total_sales desc
limit 5;

-- query 06: Assessing purchase amounts by city category and marital status over the past 6 months
select c.city_category, c.marital_status, t.month,
sum(f.purchase_amount) as total_purchase
from salefact f
join customer_dim c on f.customer_id = c.customer_id
join time_dim t on f.date_id = t.date_id
where t.full_date >= date_sub('2020-12-31', interval 6 month)
group by c.city_category, c.marital_status, month;

-- query 07: average purchase amount based on years stayed in the city and gender
select c.stay_in_current_city_years, c.gender,
avg(f.purchase_amount) as avg_purchase
from salefact f
join customer_dim c on f.customer_id = c.customer_id
group by c.stay_in_current_city_years, c.gender;

-- query 08: Ranking the top 5 city categories by revenue, grouped by product category
select c.city_category, p.product_category,
sum(f.purchase_amount) as total_revenue
from salefact f
join customer_dim c on f.customer_id = c.customer_id
join product_dim p on f.product_id = p.product_id
group by c.city_category, p.product_category
order by total_revenue desc
limit 5;

-- query 09: Measuring month-over-month sales growth 
-- percentage for each product category in the current year (again 2020)

-- query 10: Comparing total sales by age group for weekends versus weekdays in the current year
select c.age,
case when t.day_of_week in ('saturday','sunday') then 'weekend' else 'weekday' end as day_type,
sum(f.purchase_amount) as total_sales
from salefact f
join customer_dim c on f.customer_id = c.customer_id
join time_dim t on f.date_id = t.date_id
where t.year = '2020'
group by c.age, day_type;



-- query 11: the top 5 products that generated the highest revenue,
-- separated by weekday and weekend sales,
-- with results grouped by month for a specified year
select p.product_category, t.month,
case when t.day_of_week in ('saturday','sunday') then 'weekend' else 'weekday' end as day_type,
sum(f.purchase_amount) as total_revenue
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
where t.year = 2020
group by p.product_category, month, day_type
order by total_revenue desc
limit 5;

-- query 12:

-- query 13: Detailed Supplier Sales Contribution by Store and Product Name
select p.store_id, p.store_name, p.supplier_id, p.supplier_name,
p.product_id, p.product_category, sum(f.purchase_amount) as total_sales
from salefact f
join product_dim p on f.product_id = p.product_id
group by p.store_id, p.store_name, p.supplier_id, p.supplier_name, p.product_id, p.product_category
order by p.store_name, p.supplier_name, p.product_id;

-- query 14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
select p.product_id, p.product_category, t.season,
sum(f.purchase_amount) as total_sales
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
group by p.product_id, p.product_category, t.season
order by p.product_id, t.season;

-- query 15: Store-Wise and Supplier-Wise Monthly Revenue Volatility

-- query 16: Top 5 Products Purchased Together Across Multiple Orders
select a.product_id as product_01,
b.product_id as product_02,
count(*) as pair_count
from salefact a
join salefact b on a.order_id = b.order_id
and a.product_id < b.product_id
group by a.product_id, b.product_id
order by pair_count desc
limit 5;

-- query 17: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
select p.store_id, p.supplier_id, p.product_id,
sum(f.purchase_amount) as total_revenue
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
where t.year = 2020
group by p.store_id, p.supplier_id, p.product_id with rollup
order by p.store_id, p.supplier_id, p.product_id;

-- query 18: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
select p.product_id,
sum(case when t.month in ('january','february','march','april','may','june')
 then f.purchase_amount end) as half_01_revenue,
sum(case when t.month in ('july','august','september','october','november','december') 
 then f.purchase_amount end) as half_02_revenue,
sum(f.purchase_amount) as yearly_revenue,
sum(case when t.month in ('january','february','march','april','may','june') 
 then f.quantity end) as half_01_qty,
sum(case when t.month in ('july','august','september','october','november','december') 
 then f.quantity end) as half_02_qty,
sum(f.quantity) as yearly_qty
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
group by p.product_id;

-- query 19: 

-- query 20: 
create view store_quarterly_sales as
select p.store_name, t.quarter,
sum(f.purchase_amount) as total_revenue
from salefact f
join product_dim p on f.product_id = p.product_id
join time_dim t on f.date_id = t.date_id
group by p.store_name, t.quarter
order by p.store_name, t.quarter;