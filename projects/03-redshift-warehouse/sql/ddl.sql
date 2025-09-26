create schema if not exists stage;
create schema if not exists dw;

create table if not exists stage.trips_raw (
  trip_id int,
  ts timestamp,
  city varchar(64),
  km float8,
  fare float8
);

create table if not exists dw.dim_city (
  city_key int identity(1,1) primary key,
  city_name varchar(64) encode zstd
);

create table if not exists dw.fact_trip (
  trip_id int primary key,
  city_key int references dw.dim_city(city_key),
  ts timestamp encode zstd,
  km float8 encode zstd,
  fare float8 encode zstd
);
