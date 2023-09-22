-- intermediate_analytics__events_wide_format.sql
-- 2nd stage intermediate model: a wide-format (pivoted) table of events data
-- from Google Analytics (one row per event)
-- Configured to build an incrementally materialised table
{{
    config(
        materialized="incremental",
        partition_by={
            "field": "event_date",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        unique_key="event_id",
        on_schema_change="fail",
        tags=["incremental", "daily"],
    )
}}

select
    event_id,
    event_date,
    event_timestamp,
    client_id,
    user_id,
    event_name,
    continent,
    country,
    device_type,
    browser,
    operating_system,
    -- rename various fields output by the pivot operation below:
    val_page_location as page_location,
    val_page_referrer as page_referrer,
    -- -- note we append the client ID to the session ID to ensure
    -- -- that the session ID will be globally unique:
    concat(val_ga_session_id, '-', client_id) as session_id,
    val_click_element_url as link_click_target,
    -- referrer information:
    traffic_source,
    traffic_medium,
    traffic_campaign,
    regexp_extract(val_page_location, 'utm_content=([^&]+)') as traffic_referrer,
from
    (  -- Subquery allows WHERE + PIVOT in the same expression
        select distinct  -- ensure we get just one row per event in the pivot output
            * except (param_id)
        from {{ ref("intermediate_analytics__events_params") }}
        {% if is_incremental() %}
            where  -- in incremental mode, add/replace data from last daily partition onwards:
                event_date >= date(_dbt_max_partition)
                -- also add/replace data originally generated
                -- today, yesterday, or the day-before-yesterday
                -- (events_intraday data can be mutated on transfer to events)
                or event_date >= date_sub(current_date(), interval 2 day)
        {% endif %}
    ) pivot (
        /*
        Pivoting typically requires aggregation of the returned values
        over their parent rows.
        Here we expect one value for each combination of param_key and
        other fields, so non-deterministically selecting ANY_VALUE is
        used to perform the 'aggregation'
        */
        any_value(param_value) as val
        for param_key in (
            -- list of keys for which we want corresponding values (PICK YOUR OWN):
            'ga_session_id', 'page_location', 'page_referrer', 'click_element_url'
        )
    )
