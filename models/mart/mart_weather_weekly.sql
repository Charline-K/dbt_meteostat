select airport_code
, cw
, avg(avg_temp_c) as avg_temp_week
, min(min_temp_c) as min_temp_week
, max(max_temp_c) as max_temp_week
, avg(precipitation_mm) as avg_precip_mm_week
, max(max_snow_mm) as max_snow_mm_week
, avg(avg_wind_direction) as avg_wind_direction_week
, avg(avg_wind_speed_kmh) as avg_wind_speed_kmh_week
, max(wind_peakgust_kmh) as wind_peakgust_kmh_week
, avg(sun_minutes) as sun_minutes_week
from {{ref("prep_weather_daily")}} pwd 
group by airport_code, cw
order by airport_code, cw