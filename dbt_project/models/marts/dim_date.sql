-- Grain: one row per calendar day — 5-year window centred on today.
-- Generated entirely in SQL; no seed file needed.

with date_spine as (
    select range::date as date_day
    from range(
        (current_date - interval '2 years')::date,
        (current_date + interval '3 years')::date,
        interval '1 day'
    )
)

select
    cast(strftime(date_day, '%Y%m%d') as integer)   as date_id,
    date_day,
    year(date_day)                                   as year,
    month(date_day)                                  as month,
    day(date_day)                                    as day_of_month,
    dayofweek(date_day)                              as day_of_week,   -- 0=Sun … 6=Sat
    dayname(date_day)                                as day_name,
    monthname(date_day)                              as month_name,
    quarter(date_day)                                as quarter,
    isodow(date_day) >= 6                            as is_weekend
from date_spine
