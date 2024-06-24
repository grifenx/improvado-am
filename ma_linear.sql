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

int_grouped_by_linear_split as (
    select
        customer_id,
        session_id,
        source,
        --- for linear model we simply split revenue to all touchpoints within attribution window
        sum(revenue) over (partition by customer_id, session_id) /
        count() over (partition by customer_id, session_id) as revenue_by_touchpoint

    from base

),

analytics_ma_last_linear as (
    select
        source as  marketing_channel,
        round(sum(revenue_by_touchpoint), 2) as channel_revenue,
        formatReadableQuantity(channel_revenue) as channel_revenue_fmt

    from int_grouped_by_linear_split

    group by 1

)

select * from analytics_ma_last_linear