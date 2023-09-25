-- Dimension table for sessions based on the first event that isn't session_start or first_visit.
with purchase_with_params as (
  select * except (ecommerce),
    ecommerce.total_item_quantity,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.shipping_value_in_usd,
    ecommerce.shipping_value,
    ecommerce.tax_value_in_usd,
    ecommerce.tax_value,
    ecommerce.unique_items,
    {{ ga4.unnest_key('event_params', 'coupon') }},
    {{ ga4.unnest_key('event_params', 'transaction_id') }},
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'tax', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'shipping', 'double_value') }},
    {{ ga4.unnest_key('event_params', 'affiliation') }}
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("purchase_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("purchase_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'purchase'
),
join_traffic_source as (
    select 
        purchase_with_params.session_key,
        purchase_with_params.client_key,
        purchase_with_params.user_pseudo_id,
        purchase_with_params.purchase_revenue,
        sessions_traffic_sources.session_source,
        sessions_traffic_sources.session_medium,
        sessions_traffic_sources.session_campaign,
        sessions_traffic_sources.session_content,
        sessions_traffic_sources.session_term,
        sessions_traffic_sources.session_default_channel_grouping,
        sessions_traffic_sources.session_source_category
    from purchase_with_params
    left join {{ref('stg_ga4__sessions_traffic_sources')}} sessions_traffic_sources using (session_key)
    where sessions_traffic_sources.session_source = 'accesstrade'
),
include_session_properties as (
    select 
        * 
    from join_traffic_source
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} using (session_key)
    {% endif %}
)

select * from include_session_properties