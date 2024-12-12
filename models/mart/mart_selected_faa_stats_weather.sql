select mfs.*
		, pwd.min_temp_c
		, pwd.max_temp_c
		, pwd.precipitation_mm
		, pwd.max_snow_mm
		, pwd.avg_wind_direction
		, pwd.avg_wind_speed_kmh
		, pwd.wind_peakgust_kmh
from {{ref("mart_faa_stats")}} mfs
left join {{ref("prep_weather_daily")}} pwd
on mfs.faa = pwd.airport_code;