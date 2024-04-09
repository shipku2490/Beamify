1. select id, date, round(count(case when click = true then 1 end)/count(*)::numeric, 2)*100 as click_through_rate from impressions group by id, date



2. with ctr as (
select a.id, b.category_id, round(count(case when click = true then 1 end)/count(*)::numeric, 2)*100 as click_through_rate from impressions a join products b on a.id=b.product_id and a.date=b.date group by id, b.category_id
), rank_ctr as (
select id, category_id, click_through_rate, dense_rank() over(order by click_through_rate desc) drank from ctr
)
select distinct category_id from rank_ctr where drank <=3



3. with click_price_table as (
select b.price, a.click from impressions a join products b on a.id=b.product_id group by b.price, a.click
)
select case when price between 0 and 5 then '0-5'
when price between 5 and 10 then '5-10'
when price between 10 and 15 then '10-15'
else '>15'
end as price_tier,
round(count(case when click = true then 1 end)/count(*)::numeric, 2)*100 as click_through_rate
from click_price_table
group by price_tier
