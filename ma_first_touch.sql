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

int_grouped_by_first_event as (
    select
        customer_id,
        session_id,
        -- taking first event for user in each session and attributing revenue to it
        argMin(source, timestamp) as first_source,
        sum(revenue) revenue_by_session

    from base

    group by 1, 2

),

analytics_ma_first_touch as (
    select
        first_source marketing_channel,
        round(sum(revenue_by_session), 2) channel_revenue,
        formatReadableQuantity(channel_revenue) as channel_revenue_fmt

    from int_grouped_by_first_event

    group by 1

)

select * from analytics_ma_first_touch