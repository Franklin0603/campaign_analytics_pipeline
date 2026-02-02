/*
    Singular Test: assert_no_future_performance_dates
    
    Purpose: Performance data should not have dates in the future
    
    Test fails if any rows are returned
*/

select
    performance_date,
    count(*) as future_records
from {{ ref('stg_performance') }}
where performance_date > current_date
group by performance_date