with departures as (
		select origin as faa
				, count(*) as unique_from_dep
				, count(sched_dep_time) as planned_tot_dep
				, sum(cancelled) as cancelled_tot_dep
				, sum(diverted) as diverted_tot_dep
				, sum(case when cancelled=0 then 1 else 0 end) as occ_tot_dep
				, count(distinct tail_number) as tail_dep
				, count(distinct airline) as unique_airlines_dep
		from {{ref("prep.flights f")}}
		group by origin
),arrivals as (
		select dest as faa
				, count(*) as unique_to
				, count(sched_arr_time) as planned_tot_arr
				, sum(cancelled) as cancelled_tot_arr
				, sum(diverted) as diverted_tot_arr
				, sum(case when cancelled=0 then 1 else 0 end) as occ_tot_arr
				, count(distinct tail_number) as tail_arr
				, count(distinct airline) as unique_airlines_arr
		from {{ref("prep.flights f")}}
		group by dest
),airport_tot_stats as (
		select faa 
				, unique_from_dep
				, unique_to
				, planned_tot_dep + planned_tot_arr as planned_tot
				, cancelled_tot_dep + cancelled_tot_arr as cancelled_tot
				, diverted_tot_dep + diverted_tot_arr as diverted_tot
				, occ_tot_dep + occ_tot_arr as occ_tot
		from departures
		join arrivals
		using (faa)
)
select a.country
		, a.city
		, a.name
		, ats.*
from airport_tot_stats ats
left join {{ref("prep.flights f")}} a
using (faa)