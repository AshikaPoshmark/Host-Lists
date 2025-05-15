


drop table if exists analytics_scratch.ashika_lifetime_top_host;
create table analytics_scratch.ashika_lifetime_top_host as
select a.*,
       CASE WHEN seller_activated_date < show_host_activated_date THEN 'Yes'
            ELSE 'No' END AS is_seller_activated_prior_to_host_activation,
        date_diff('days',DATE(joined_date),DATE(seller_activated_date))                               as seller_activated_days_since_joined,
        date_diff('days',DATE(seller_activated_date),DATE(show_host_activated_date))                   as host_activated_days_since_seller_activated,
        date_diff('days',DATE(joined_date),DATE(show_host_activated_date))                              as host_activated_days_since_joined,
        date_diff('days',DATE(show_host_activated_date),DATE(first_time_they_reached_seg_4_or_5))      as first_reached_seg_4or_5_since_host_activated,
        date_diff('days',DATE(show_host_activated_date),DATE(live_show_host_activated_date))      as live_show_host_activated_days_since_host_activation,
        date_diff('days',DATE(show_host_activated_date),current_date)      as days_since_host_activated,
        date_diff('days',DATE(last_show_hosted_date),current_date)      as days_since_last_show_hosted


    FROM

    (select  id as host_id,username,
        DATE(joined_at) as joined_date,
        DATE(seller_activated_at) as seller_activated_date,
        DATE(show_host_activated_at) as show_host_activated_date,
        DATE(live_show_host_activated_at) as live_show_host_activated_date,
        last_show_hosted_date,
        min(start_date) as first_time_they_reached_seg_4_or_5
       from analytics.dw_user_segments

         left join analytics.dw_users on dw_user_segments.id = dw_users.user_id
         left join analytics.dw_users_cs on dw_user_segments.id = dw_users_cs.user_id
       left join analytics.dw_users_info on dw_user_segments.id = dw_users_info.user_id
       left join (select creator_id, max(DATE(start_at)) as last_show_hosted_date from analytics.dw_shows group by 1 ) on id =creator_id


        where user_segment_daily in ('Segment 4: $25k to $200k','Segment 5: >=$200k') and user_type= 'show_host'
        group by 1,2,3,4,5,6,7) as a;




------------------- 

Select a.*,
       case when dw_orders.show_id is not null and host_id=creator_id then 'show_order'
            when dw_orders.show_id is null then 'platform_order'
            else 'sold_by_other_hosts_through_st_show' end as order_type,
    count(distinct dw_orders.show_id) as overall_shows_with_orders,
    count( order_id) over_all_orders,
    sum(order_number_items) over_all_order_items,
    sum(coalesce(order_gmv_usd*0.01,0)) as over_all_gmv,

    count(distinct case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then dw_orders.show_id end) as shows_with_orders_in_first_30_days_of_host_activation,
    count(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then order_id end) as orders_in_first_30_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then order_number_items end)order_items_in_first_30_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then coalesce(order_gmv_usd*0.01,0) end) as gmv_in_first_30_days_of_host_activation,

    count(distinct case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then dw_orders.show_id end) as shows_with_orders_in_first_365_days_of_host_activation,
    count(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then order_id end) as orders_in_first_365_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then order_number_items end)order_items_in_first_365_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then coalesce(order_gmv_usd*0.01,0) end) as gmv_in_first_365_days_of_host_activation

from analytics_scratch.ashika_lifetime_top_host as a
left join analytics.dw_orders on seller_id=host_id
left join (select show_id,creator_id from analytics.dw_shows) as dw_shows on dw_shows.show_id = dw_orders.show_id
where is_seller_activated_prior_to_host_activation = 'No'
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15



------------------  create ashika_365_top_host_seller_activated_prior_to_host_activation from  ashika_lifetime_top_host


drop table if exists analytics_scratch.ashika_365_top_host_seller_activated_prior_to_host_activation;
create table analytics_scratch.ashika_365_top_host_seller_activated_prior_to_host_activation as

Select a.*,
       case when dw_orders.booked_at >= show_host_activated_date and dw_orders.booked_at < date_add('days',365,show_host_activated_date)
            then 'post_365_days_of_host_activation'
            when  dw_orders.booked_at >= date_add('days',-365,show_host_activated_date) and dw_orders.booked_at < show_host_activated_date
            then  'prior_365_days_of_host_activation' end as post_pre_host_activation,

       case when dw_orders.show_id is not null and host_id=host_id_as_seller_id_for_shows and host_id = seller_id  then 'show_order_own_listing'
            when dw_orders.show_id is not null and host_id=host_id_as_seller_id_for_shows  and host_id != seller_id then 'show_order_community_listing'
            when dw_orders.show_id is null then 'platform_order'
         end as sold_type,

    count(distinct dw_orders.show_id) as overall_shows_with_orders,
    count( order_id) over_all_orders,
    sum(order_number_items) over_all_order_items,
    sum(coalesce(order_gmv_usd*0.01,0)) as over_all_gmv

from analytics_scratch.ashika_lifetime_top_host as a
left join (select dw_orders.order_id,order_number_items,order_gmv_usd,dw_orders.show_id,creator_id,seller_id, booked_at,
       case when dw_orders.show_id is not null then creator_id
            else seller_id end as host_id_as_seller_id_for_shows
       from analytics.dw_orders
left join analytics.dw_shows on dw_shows.show_id = dw_orders.show_id) as dw_orders on host_id_as_seller_id_for_shows = host_id

where post_pre_host_activation is not null
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;


-----------------   Hosts with no poshmark history as sellers:raw data with activation dates 365 and overall GMV details  -------------


    Select a.*,
       case when joined_date < '2022-09-22' then 'yes'
                else 'no' end as is_joined_poshmark_before_shows_introduced,

    case when first_reached_seg_4or_5_since_host_activated <= 10 then 'within_10_days'
            when first_reached_seg_4or_5_since_host_activated > 10 and first_reached_seg_4or_5_since_host_activated < 31 then 'within_10_to_30_days'
            when first_reached_seg_4or_5_since_host_activated > 30 and first_reached_seg_4or_5_since_host_activated < 91 then 'within__30_to_90_days'
            when first_reached_seg_4or_5_since_host_activated > 90 and first_reached_seg_4or_5_since_host_activated < 121 then 'within__90_to_120_days'
            when first_reached_seg_4or_5_since_host_activated > 120 and first_reached_seg_4or_5_since_host_activated < 365 then 'within__120_to_365_days'
            when first_reached_seg_4or_5_since_host_activated > 364 and first_reached_seg_4or_5_since_host_activated < 500 then 'within__365_to_500_days'
            else 'more_than_500_days' end as first_to_become_a_top_host_after_host_activation,

case when
date_diff('days',DATE(last_show_hosted_date),current_date) > 365   then 'More than 12 months since last show hosted'
when date_diff('days',DATE(last_show_hosted_date),current_date) > 120 then 'More than 6 to 12 months since last show hosted'
when date_diff('days',DATE(last_show_hosted_date),current_date) >30 then 'More than 1 to 6 months since  last show hosted'
else 'active_host' end as months_since_host_have_done_their_last_show,

        case when dw_orders.show_id is not null and host_id=host_id_as_seller_id_for_shows and host_id = seller_id  then 'show_order_own_listing'
            when dw_orders.show_id is not null and host_id=host_id_as_seller_id_for_shows  and host_id != seller_id then 'show_order_community_listing'
            when dw_orders.show_id is null then 'platform_order'
         end as sold_type,

    count(distinct dw_orders.show_id) as overall_shows_with_orders,
    count( order_id) over_all_orders,
    sum(order_number_items) over_all_order_items,
    sum(coalesce(order_gmv_usd*0.01,0)) as over_all_gmv,

    count(distinct case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then dw_orders.show_id end) as shows_with_orders_in_first_30_days_of_host_activation,
    count(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then order_id end) as orders_in_first_30_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then order_number_items end)order_items_in_first_30_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',30,show_host_activated_date) then coalesce(order_gmv_usd*0.01,0) end) as gmv_in_first_30_days_of_host_activation,

    count(distinct case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then dw_orders.show_id end) as shows_with_orders_in_first_365_days_of_host_activation,
    count(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then order_id end) as orders_in_first_365_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then order_number_items end)order_items_in_first_365_days_of_host_activation,
    sum(case when dw_orders.booked_at < date_add('days',365,show_host_activated_date) then coalesce(order_gmv_usd*0.01,0) end) as gmv_in_first_365_days_of_host_activation

from analytics_scratch.ashika_lifetime_top_host as a
left join (select dw_orders.order_id,order_number_items,order_gmv_usd,dw_orders.show_id,creator_id,seller_id, booked_at,
       case when dw_orders.show_id is not null then creator_id
            else seller_id end as host_id_as_seller_id_for_shows
       from analytics.dw_orders
left join analytics.dw_shows on dw_shows.show_id = dw_orders.show_id) as dw_orders on host_id_as_seller_id_for_shows = host_id

where is_seller_activated_prior_to_host_activation = 'No'
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20;





-----------------  Hosts with poshmark history as sellers GMV  ------------------------------------



select
    host_id,
    username,
    is_seller_activated_prior_to_host_activation,
    case when seller_activated_date < '2022-09-22' then 'yes'
                else 'no' end as is_seller_activated_before_shows_introduced,

    case when first_reached_seg_4or_5_since_host_activated <= 10 then 'within_10_days'
            when first_reached_seg_4or_5_since_host_activated > 10 and first_reached_seg_4or_5_since_host_activated < 31 then 'within_10_to_30_days'
            when first_reached_seg_4or_5_since_host_activated > 30 and first_reached_seg_4or_5_since_host_activated < 91 then 'within__30_to_90_days'
            when first_reached_seg_4or_5_since_host_activated > 90 and first_reached_seg_4or_5_since_host_activated < 121 then 'within__90_to_120_days'
            when first_reached_seg_4or_5_since_host_activated > 120 and first_reached_seg_4or_5_since_host_activated < 365 then 'within__120_to_365_days'
            when first_reached_seg_4or_5_since_host_activated > 364 and first_reached_seg_4or_5_since_host_activated < 500 then 'within__365_to_500_days'
            else 'more_than_500_days' end as first_to_become_a_top_host_after_host_activation,

case when
days_since_last_show_hosted > 365   then 'More than 12 months since last show hosted'
when days_since_last_show_hosted > 120 then 'More than 6 to 12 months since last show hosted'
when days_since_last_show_hosted >30 then 'More than 1 to 6 months since  last show hosted'
else 'active_host' end as months_since_host_have_done_their_last_show,



    sum(case when post_pre_host_activation = 'prior_365_days_of_host_activation' then coalesce(over_all_gmv,0) end) as gmv_prior_365_days_of_host_activation,
    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'show_order_own_listing' then coalesce(over_all_gmv,0) end) as gmv_post_365_days_of_host_activation_show_own_listings,
    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'show_order_community_listing' then coalesce(over_all_gmv,0) end)  as gmv_post_365_days_of_host_activation_show_community_listings,
    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'platform_order' then coalesce(over_all_gmv,0) end ) as gmv_post_365_days_of_host_activation_non_show_platform_order
    from analytics_scratch.ashika_365_top_host_seller_activated_prior_to_host_activation
       group by 1,2,3,4,5,6





-----------------  Hosts with no poshmark history as sellers GMV ------------------



Select a.host_id,
       a.username,
       a.is_seller_activated_prior_to_host_activation,
       case when joined_date < '2022-09-22' then 'yes'
                else 'no' end as is_joined_poshmark_before_shows_introduced,

    case when first_reached_seg_4or_5_since_host_activated <= 10 then 'within_10_days'
            when first_reached_seg_4or_5_since_host_activated > 10 and first_reached_seg_4or_5_since_host_activated < 31 then 'within_10_to_30_days'
            when first_reached_seg_4or_5_since_host_activated > 30 and first_reached_seg_4or_5_since_host_activated < 91 then 'within__30_to_90_days'
            when first_reached_seg_4or_5_since_host_activated > 90 and first_reached_seg_4or_5_since_host_activated < 121 then 'within__90_to_120_days'
            when first_reached_seg_4or_5_since_host_activated > 120 and first_reached_seg_4or_5_since_host_activated < 365 then 'within__120_to_365_days'
            when first_reached_seg_4or_5_since_host_activated > 364 and first_reached_seg_4or_5_since_host_activated < 500 then 'within__365_to_500_days'
            else 'more_than_500_days' end as first_to_become_a_top_host_after_host_activation,

case when
days_since_last_show_hosted > 365   then 'More than 12 months since last show hosted'
when days_since_last_show_hosted > 120 then 'More than 6 to 12 months since last show hosted'
when days_since_last_show_hosted >30 then 'More than 1 to 6 months since  last show hosted'
else 'active_host' end as months_since_host_have_done_their_last_show,

    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'show_order_own_listing' then coalesce(over_all_gmv,0) end) as gmv_post_365_days_of_host_activation_show_own_listings,
    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'show_order_community_listing' then coalesce(over_all_gmv,0) end)  as gmv_post_365_days_of_host_activation_show_community_listings,
    sum(case when post_pre_host_activation = 'post_365_days_of_host_activation' and sold_type = 'platform_order' then coalesce(over_all_gmv,0) end ) as gmv_post_365_days_of_host_activation_non_show_platform_order

from analytics_scratch.ashika_365_top_host_seller_activated_prior_to_host_activation as a
where is_seller_activated_prior_to_host_activation = 'No'
group by  1,2,3,4,5,6




------- -----  Table 1 : Count of Hosts with Poshmark history as sellers and not



Select Count(host_id) ,
       Count(CASE WHEN is_seller_activated_prior_to_host_activation = 'Yes' THEN host_id END) AS hosts_who_were_seller_activated,
       Count(CASE WHEN is_seller_activated_prior_to_host_activation = 'No' THEN host_id END) AS seller_activated_after_becoming_host
from analytics_scratch.ashika_lifetime_top_host


--------------   Table 3: Time taken by the hosts to reach the Top segment (4 or 5) for the first time.

select case when first_reached_seg_4or_5_since_host_activated <= 10 then 'atleast_10_days'
            when first_reached_seg_4or_5_since_host_activated > 10 and first_reached_seg_4or_5_since_host_activated < 31 then 'between_10_to_30_days'
            when first_reached_seg_4or_5_since_host_activated > 30 and first_reached_seg_4or_5_since_host_activated < 91 then 'between_30_to_90_days'
            when first_reached_seg_4or_5_since_host_activated > 90 and first_reached_seg_4or_5_since_host_activated < 121 then 'between_90_to_120_days'
            when first_reached_seg_4or_5_since_host_activated > 120 and first_reached_seg_4or_5_since_host_activated < 365 then 'between_120_to_365_days'
            when first_reached_seg_4or_5_since_host_activated > 364 and first_reached_seg_4or_5_since_host_activated < 500 then 'between_120_to_500_days'
            else 'more_than_500_days' end as first_to_become_a_top_host_after_host_activation,
    case when seller_activated_date < '2022-09-22' then 'yes'
                else 'no' end as is_seller_activated_before_shows_introduced,
count(distinct host_id)
       from analytics_scratch.ashika_365_top_host_seller_activated_prior_to_host_activation
       group by 1,2
