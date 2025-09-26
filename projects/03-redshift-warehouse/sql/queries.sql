-- Sample analytics
select city_name, count(*) trips, avg(fare) avg_fare
from dw.fact_trip f join dw.dim_city d on f.city_key = d.city_key
group by 1 order by trips desc;
