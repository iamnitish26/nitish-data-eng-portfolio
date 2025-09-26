create or replace procedure dw.sp_merge_dim_city()
language plpgsql
as $$
begin
  insert into dw.dim_city (city_name)
  select distinct city
  from stage.trips_raw r
  left join dw.dim_city d on d.city_name = r.city
  where d.city_key is null;
end;
$$;

create or replace procedure dw.sp_load_fact_trip()
language plpgsql
as $$
begin
  -- upsert trips
  delete from dw.fact_trip f using stage.trips_raw s where f.trip_id = s.trip_id;
  insert into dw.fact_trip (trip_id, city_key, ts, km, fare)
  select r.trip_id, d.city_key, r.ts, r.km, r.fare
  from stage.trips_raw r
  join dw.dim_city d on d.city_name = r.city;
end;
$$;
