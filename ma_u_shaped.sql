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
        --- adding event_index and flag whether it was revenue-generating event for further use
        row_number() over (partition by customer_id, session_id order by timestamp) as event_index,
        if(revenue > 0, true, false) as revenue_event,
        timestamp

    from {{ ref('marketing_dummy_data') }}

),

/* since we defined attribution window as a single session,
   we will need to drop any events that occur after revenue-generating
   event withing single session so they won't mess with calculations */
int_get_revenue_event_index as (
    select
        customer_id,
        session_id,
        event_index

    from base

    where revenue_event = true

),

--- self join to identify rows that should be included in calculation
int_get_flags_for_calculation as (
    select
        base.customer_id,
        base.session_id,
        base.source,
        base.revenue,
        count() over (partition by customer_id, session_id) as session_events_cnt,
        base.event_index,
        base.revenue_event,
        if(int_get_revenue_event_index.event_index >= base.event_index, true, false) as calculation_event

    from base

    left join int_get_revenue_event_index
        on int_get_revenue_event_index.customer_id = base.customer_id
        and int_get_revenue_event_index.session_id = base.session_id

),

int_split_revenue_u_shape as (
    select
        customer_id,
        session_id,
        source,
        multiIf(
            event_index = 1 and revenue_event = true, 1.0, --- revenue shouldn't be split if purchase happens at start
            session_events_cnt = 1, 1.0, --- if there's single event in session we attribute session revenue to it
            session_events_cnt = 2, 0.5, --- if 2 events - we split evenly
            event_index = 1, 0.4, --- if there are more than 2, we actually start attributing 40% to first event
            revenue_event = true, 0.4, --- 40% to last event, leading to purchase
            0.2 / nullIf(toFloat64(session_events_cnt - 2), 0) --- 20% split evenly among remaining in between
        ) as weight,
        --- getting sessions total revenue for further split
        sum(revenue) over (partition by customer_id, session_id) as session_revenue,
        session_revenue * weight as u_revenue

    from int_get_flags_for_calculation

    where calculation_event = true
),

analytics_ma_u_shaped as (
    select
        source as marketing_channel,
        round(sum(u_revenue), 2) as channel_revenue,
        formatReadableQuantity(channel_revenue) as channel_revenue_fmt

    from int_split_revenue_u_shape

    group by 1

)

select * from analytics_ma_u_shaped