with

base as (

    select
        customer_id,
        session_id,
        extractURLParameter(url, 'source') as source,
        extractURLParameter(url, 'medium') as medium,
        nullIf(extractURLParameter(url, 'campaign'), '') as campaign,
        nullIf(extractURLParameter(url, 'content'), '') as content,
        nullIf(extractURLParameter(url, 'term'), '') as term,
        revenue,
        timestamp

    from {{ ref('marketing_dummy_data') }}

),

int_grouped_by_last_revenue_event as (
    select
        customer_id,
        session_id,
        --- grabbing last event in every session where purchase happened.
        --- for last non-direct touch we can add "and source != Direct" to if case
        argMaxIf(source, timestamp, revenue > 0) as last_source,
        sum(revenue) as revenue_by_session

    from base

    group by 1, 2
    --- filtering out sessions with 0 revenue to skip empty rows in the result
    having revenue_by_session > 0

),

analytics_ma_last_touch as (
    select
        last_source as  marketing_channel,
        round(sum(revenue_by_session), 2) as channel_revenue,
        formatReadableQuantity(channel_revenue) as channel_revenue_fmt

    from int_grouped_by_last_revenue_event

    group by 1

)

select * from analytics_ma_last_touch