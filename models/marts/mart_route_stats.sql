with routes as (
	select origin
		, dest
		, count(*) as no_total_flights_per_route
		, count(distinct airline) as unique_airline
		, count(distinct flight_number) as unique_airplane
		, avg(actual_elapsed_time) as avg_elapsed_time
		, avg(arr_delay) as avg_delay
		, max(arr_delay) as max_arr_delay
		, min(arr_delay) as min_arr_delay
		, max(dep_delay) as max_dep_delay
		, min(dep_delay) as min_dep_delay
		, sum(cancelled) as cancel_tot
		, sum(diverted) as divert_tot
	from {{ref("prep_flights")}}
	group by origin, dest
), routes_join_origin as (
	select 
		a.country as origin_country
		, a.city as origin_city
		, a.name as origin_name
		, r.*
	from routes r
	join {{ref("prep_airports")}} a
	on r.origin = a.faa
)
select 
	a.country as dest_country
	, a.city as dest_city
	, a.name as dest_name
	, rjo.*
from routes_join_origin rjo
join {{ref("prep_airports")}} a
on rjo.dest = a.faa