--- helper table to further develop daily data
CREATE VIEW daily_counts AS
SELECT date_dim.date_key,
    date_dim.full_date,
    date_dim.month_name,
    date_dim.day,
    date_dim.day_name,
    date_dim.weekend,
    count(rides.id) AS ride_totals,
    count(trip_demo.user_type) FILTER (WHERE trip_demo.user_type = 'Subscriber') AS subscriber_rides,
    count(trip_demo.user_type) FILTER (WHERE trip_demo.user_type = 'Customer') AS customer_rides,
    count(trip_demo.user_type) FILTER (WHERE trip_demo.user_type = 'Unknown') AS unknown_rides,
    count(rides.valid_duration) FILTER (WHERE NOT rides.valid_duration) AS late_return
FROM rides
    RIGHT JOIN date_dim ON rides.date_key = date_dim.date_key
    LEFT JOIN trip_demo ON rides.trip_demo = trip_demo.id
GROUP BY date_dim.date_key
ORDER BY date_dim.date_key;

--- table with daily data, perfect to analyze engagement and weather conditions
CREATE VIEW daily_data AS
SELECT daily_counts.date_key,
    daily_counts.full_date,
    daily_counts.month_name,
    daily_counts.day,
    daily_counts.ride_totals,
    sum(daily_counts.ride_totals) OVER (PARTITION BY daily_counts.month_name ORDER BY daily_counts.date_key) AS month_running_total,
    daily_counts.subscriber_rides,
    daily_counts.customer_rides,
    daily_counts.unknown_rides,
    daily_counts.late_return,
    daily_counts.weekend,
    weather.min_temp,
    weather.avg_temp,
    weather.max_temp,
    weather.avg_wind,
    weather.precipitation,
    weather.snow_amt,
    weather.rain,
    weather.snow
FROM daily_counts
    JOIN weather ON daily_counts.date_key = weather.date_key
ORDER BY daily_counts.date_key;

--- table with monthly data, perfect for financial analysis and growth projection
CREATE VIEW monthly_data AS
SELECT date_dim.month,
    date_dim.month_name,
    ROUND(AVG(daily_data.ride_totals)) AS avg_daily_rides,
    SUM(daily_data.ride_totals) AS total_rides,
    ROUND(AVG(daily_data.customer_rides)) AS avg_customer_rides,
    SUM(daily_data.customer_rides) AS total_customer_rides,
    ROUND(AVG(daily_data.subscriber_rides)) AS avg_subscriber_rides,
    SUM(daily_data.subscriber_rides) AS total_subscriber_rides,
    ROUND(AVG(daily_data.unknown_rides)) AS avg_unknown_rides,
    SUM(daily_data.unknown_rides) AS total_unknown_rides,
    SUM(daily_data.late_return) AS total_late_returns,
    ROUND(AVG(daily_data.avg_temp)) AS monthly_avg_temp,
    COUNT(daily_data.snow) FILTER (WHERE daily_data.snow) AS days_with_snow,
    COUNT(daily_data.rain) FILTER (WHERE daily_data.rain) AS days_with_rain,
    MAX(daily_data.snow_amt) AS max_snow_amt,
    MAX(daily_data.precipitation) AS max_precipitation
FROM date_dim
    JOIN daily_data ON date_dim.date_key = daily_data.date_key
GROUP BY date_dim.month, date_dim.month_name
ORDER BY date_dim.month;

--- table with late returns to analyze rides that went beyond the time limit
CREATE VIEW late_returns AS
SELECT date_dim.full_date,
    rides.id,
    rides.trip_hours,
    rides.bike_id,
    (SELECT stations.station_name
        FROM stations
        WHERE rides.start_station_id = stations.id) AS start_location,
    (SELECT stations.station_name
        FROM stations
        WHERE rides.end_station_id = stations.id) AS end_location,
    trip_demo.user_type
FROM rides
    JOIN date_dim ON rides.date_key = date_dim.date_key
    JOIN trip_demo ON rides.trip_demo = trip_demo.id
WHERE rides.valid_duration = False;

--- week summary over the whole year
CREATE VIEW week_summary AS
SELECT daily.day_name,
    ROUND(AVG(daily.ride_totals)) AS avg_rides,
    ROUND(AVG(daily.customer_rides)) AS avg_customer_rides,
    ROUND(AVG(daily.subscriber_rides)) AS avg_subscriber_rides
FROM daily_counts AS daily
GROUP BY daily.day_name
ORDER BY (ROUND(AVG(daily.ride_totals))) DESC;

--- hourly summary over the whole year
CREATE VIEW hourly_summary AS
SELECT EXTRACT(HOUR FROM rides.start_time) AS start_hour,
    COUNT(*) AS ride_totals
FROM rides
GROUP BY start_hour
ORDER BY start_hour DESC;

--- demographics summary
CREATE VIEW trip_demographics AS
SELECT COUNT(rides.id) AS total_rides,
    demo.age,
    demo.gender,
    demo.user_type
FROM rides
    JOIN trip_demo AS demo ON rides.trip_demo = demo.id
GROUP BY demo.age, demo.gender, demo.user_type
ORDER BY demo.user_type, demo.gender, demo.age;