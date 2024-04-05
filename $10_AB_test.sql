




create temporary table jk_basic_10_pros as (
    select *
    from (
      select
        provider_id
        , test_group
        , event_date
        , row_number() over (partition by provider_id order by event_date asc) event_num
      from styleseat_events.styleseat_events_master
      where event = 'client_ab_test_assignment'
        and test_name = 'test_pro_basic_10_pricing_fly4076_230825'
        and event_date > '2023-04-12'
    )
    where event_num = 1
  )
;



select
	provider_id
	, test_group
    , event_date :: date 
    -- , row_number() over (partition by provider_id order by event_date asc) event_num
from styleseat_events.styleseat_events_master
where event = 'client_ab_test_assignment'
	and test_name = 'test_pro_basic_10_pricing_fly4076_230825'
    and event_date >= '2023-09-01'
QUALIFY  row_number() over (partition by provider_id order by event_date asc) =1



select *
from jk_basic_10_pros 
limit 190;


select event_date :: date , test_group, count( provider_id) -- distinct
from jk_basic_10_pros 
group by 1,2
order by 1,2




-----  -----  -----  -----  


with jc_pro_iteration as (
  select
    pd.id provider_id
    , date(pd.creation_time) signup_dt
    -- p1c
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '7 day' then 1 else 0 end p1c_7d
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '24 hour' then 1 else 0 end p1c_24h
    -- availability
    , max(case when pwf.provider_id is not null then 1 end) added_availability_7d
    , max(case when pwf.start_date between pd.creation_time and pd.creation_time + interval '24 hour' and pwf.provider_id is not null then 1 end) added_availability_24h
    -- images
    , max(case when i.provider_id is not null then 1 end) added_images_7d
    , max(case when i.creation_time between pd.creation_time and pd.creation_time + interval '24 hour' and i.provider_id is not null then 1 end) added_images_24h
    -- services
    , max(case when psd.provider_id is not null then 1 end) added_services_7d
    , max(case when psd.creation_time between pd.creation_time and pd.creation_time + interval '24 hour' and psd.provider_id is not null then 1 end) added_services_24h
    -- location
    , max(case when pld.provider_id is not null then 1 end) added_location_ever
    -- appts: active, engaged
    , max(case when adf.provider_id is not null and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_7d
    , max(case when adf.provider_id is not null and booking_type in ('guest_user_booking', 'client_user_booking')  and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_client_booking_7d
    , max(case when adf.provider_id is not null and is_ncd = 1 and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_ncc_booking_7d
    -- autocharge enabled time (payments)
    , max(case when pd.autocharge_enabled_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 else 0 end) payments_7d
    , max(case when pd.autocharge_enabled_time between pd.creation_time and pd.creation_time + interval '14 day' then 1 else 0 end) payments_14d
    -- engagement
    , count(distinct case when adf.creation_time between pd.creation_time and pd.creation_time + interval '30 day' then adf.appointment_id end) f30d_bookings
    -- plan selection
    , max(case when pcd.subscription_state ilike '%scheduling%' then 1 else 0 end) chose_scheduling_plan_7d
  from provider_dim pd
  left join provider_change_dim pcd
    on pd.id = pcd.provider_id
      and pd.creation_time + interval '7 day' between pcd.effective_start_date and pcd.effective_end_date
  left join provider_workperiod_fact pwf
    on pd.id = pwf.provider_id
      and pwf.start_date between pd.creation_time and pd.creation_time + interval '7 day'
  left join image_dim i
      on i.provider_id = pd.id
        and i.creation_time between pd.creation_time and pd.creation_time + interval '7 day'
        and i.deletion_time is null
        and i.privacy_status = 1
        and i.upload_type not in (3)
  left join provider_service_dim psd
      on pd.id = psd.provider_id
        and psd.creation_time between pd.creation_time and pd.creation_time + interval '7 day'
  left join provider_location_dim pld on pld.provider_id = pd.id
  left join appointments_details_fact adf
    on adf.provider_id = pd.id
      and adf.creation_time between pd.creation_time and pd.creation_time + interval '30 day'
  left join (
    select provider_id, min(as_of_date) first_engaged_dt
    from analytics.jc_pro_segmentation
    group by 1
  ) ps on ps.provider_id = pd.id
  where pd.creation_time > current_date - interval '180 day'
    and pd.type = 1
    and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
    and pd.status <>'staff'
    and exclude_from_reporting <>1
  group by 1,2,3,4
),

jk_basic_10_pros as (
select
	provider_id
	, test_group
    , event_date :: date 
    -- , row_number() over (partition by provider_id order by event_date asc) event_num
from styleseat_events.styleseat_events_master
where event = 'client_ab_test_assignment'
	and test_name = 'test_pro_basic_10_pricing_fly4076_230825'
    and event_date >= '2023-09-01'
QUALIFY  row_number() over (partition by provider_id order by event_date asc) =1

 )


select
    signup_dt
    , tst.Test_group
    , date(date_trunc('week', signup_dt) + interval '6 day') wk_end_dt
    , count(distinct a.provider_id) pros
    , count(distinct case when p1c_7d = 1 then a.provider_id end) p1c_7d
    , count(distinct case when p1c_24h = 1 then a.provider_id end) p1c_24h
    , count(distinct case when added_availability_7d = 1 then a.provider_id end) added_availability_7d
    , count(distinct case when added_availability_24h = 1 then a.provider_id end) added_availability_24h
    , count(distinct case when added_images_7d = 1 then a.provider_id end) added_images_7d
    , count(distinct case when added_images_24h = 1 then a.provider_id end) added_images_24h
    , count(distinct case when added_services_7d = 1 then a.provider_id end) added_services_7d
    , count(distinct case when added_services_24h = 1 then a.provider_id end) added_services_24h
    , count(distinct case when added_location_ever = 1 then a.provider_id end) added_location_ever
    , count(distinct case when active_7d = 1 then a.provider_id end) active_7d
    , count(distinct case when active_client_booking_7d = 1 then a.provider_id end) active_client_booking_7d
    , count(distinct case when active_ncc_booking_7d = 1 then a.provider_id end) active_ncc_booking_7d
    , sum(case when adf.start between a.signup_dt and a.signup_dt + interval '7 day' then adf.ss_take_revenue end) revenue_7d
    , sum(case when adf.start between a.signup_dt and a.signup_dt + interval '14 day' then adf.ss_take_revenue end) revenue_14d
    , count(distinct case when payments_7d = 1 then a.provider_id end) payments_7d
    , count(distinct case when payments_14d = 1 then a.provider_id end) payments_14d
    , count(distinct case when f30d_bookings >= 6 then a.provider_id end) engaged_30d
    , count(distinct case when chose_scheduling_plan_7d = 1 and p1c_7d = 1 then a.provider_id end) chose_scheduling_plan_after_p1c_7d
    , sum(case when adf.start between a.signup_dt and a.signup_dt + interval '30 day' then adf.ss_take_revenue end) revenue_30d
    --, count(case when adf.ss_take_revenue = 0 then a.provider_id end ) cnt_0_rev
    --, max(adf.ss_take_revenue) max_rev
  from jc_pro_iteration a
  join jk_basic_10_pros tst on  tst.provider_id = a.provider_id and a.signup_dt = tst.event_date
  left join appointments_details_fact adf
    on adf.provider_id = a.provider_id
      and adf.start between a.signup_dt and a.signup_dt + interval '30 day'
  where signup_dt < current_date
  group by 1,2,3
  order by 1 desc, 2 desc
;


--------------------------------------------------

-- side questions 

with jc_pro_iteration as (
  select
    pd.id provider_id
    , date(pd.creation_time) signup_dt
    -- p1c
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '7 day' then 1 else 0 end p1c_7d
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '24 hour' then 1 else 0 end p1c_24h
    -- availability
    , max(case when pwf.provider_id is not null then 1 end) added_availability_7d
    , max(case when pwf.start_date between pd.creation_time and pd.creation_time + interval '24 hour' and pwf.provider_id is not null then 1 end) added_availability_24h
    -- images
    , max(case when i.provider_id is not null then 1 end) added_images_7d
    , max(case when i.creation_time between pd.creation_time and pd.creation_time + interval '24 hour' and i.provider_id is not null then 1 end) added_images_24h
    -- services
    , max(case when psd.provider_id is not null then 1 end) added_services_7d
    , max(case when psd.creation_time between pd.creation_time and pd.creation_time + interval '24 hour' and psd.provider_id is not null then 1 end) added_services_24h
    -- location
    , max(case when pld.provider_id is not null then 1 end) added_location_ever
    -- appts: active, engaged
    , max(case when adf.provider_id is not null and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_7d
    , max(case when adf.provider_id is not null and booking_type in ('guest_user_booking', 'client_user_booking')  and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_client_booking_7d
    , max(case when adf.provider_id is not null and is_ncd = 1 and adf.creation_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 end) active_ncc_booking_7d
    -- autocharge enabled time (payments)
    , max(case when pd.autocharge_enabled_time between pd.creation_time and pd.creation_time + interval '7 day' then 1 else 0 end) payments_7d
    , max(case when pd.autocharge_enabled_time between pd.creation_time and pd.creation_time + interval '14 day' then 1 else 0 end) payments_14d
    -- engagement
    , count(distinct case when adf.creation_time between pd.creation_time and pd.creation_time + interval '30 day' then adf.appointment_id end) f30d_bookings
    -- plan selection
    , max(case when pcd.subscription_state ilike '%scheduling%' then 1 else 0 end) chose_scheduling_plan_7d
  from provider_dim pd
  left join provider_change_dim pcd
    on pd.id = pcd.provider_id
      and pd.creation_time + interval '7 day' between pcd.effective_start_date and pcd.effective_end_date
  left join provider_workperiod_fact pwf
    on pd.id = pwf.provider_id
      and pwf.start_date between pd.creation_time and pd.creation_time + interval '7 day'
  left join image_dim i
      on i.provider_id = pd.id
        and i.creation_time between pd.creation_time and pd.creation_time + interval '7 day'
        and i.deletion_time is null
        and i.privacy_status = 1
        and i.upload_type not in (3)
  left join provider_service_dim psd
      on pd.id = psd.provider_id
        and psd.creation_time between pd.creation_time and pd.creation_time + interval '7 day'
  left join provider_location_dim pld on pld.provider_id = pd.id
  left join appointments_details_fact adf
    on adf.provider_id = pd.id
      and adf.creation_time between pd.creation_time and pd.creation_time + interval '30 day'
  left join (
    select provider_id, min(as_of_date) first_engaged_dt
    from analytics.jc_pro_segmentation
    group by 1
  ) ps on ps.provider_id = pd.id
  where pd.creation_time > current_date - interval '180 day'
    and pd.type = 1
    and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
    and pd.status <>'staff'
    and exclude_from_reporting <>1
  group by 1,2,3,4
),

jk_basic_10_pros as (
select
	provider_id
	, test_group
    , event_date :: date 
    -- , row_number() over (partition by provider_id order by event_date asc) event_num
from styleseat_events.styleseat_events_master
where event = 'client_ab_test_assignment'
	and test_name = 'test_pro_basic_10_pricing_fly4076_230825'
    and event_date >= '2023-09-01'
QUALIFY  row_number() over (partition by provider_id order by event_date asc) =1

 )
 
 
 /*
 select *
   from jc_pro_iteration a
  join jk_basic_10_pros tst on  tst.provider_id = a.provider_id and a.signup_dt = tst.event_date
  limit 190
  
 */

/*
select  Test_group,
	avg(ss_rev) av,
	--median(ss_rev) med,
	round(variance(ss_rev)) var,
	-- round(var_samp(ss_rev)) varsamp,
	-- round(var_pop(ss_rev) )varpop,
	round(STDDEV(ss_rev)) stdev,
	count(distinct provider_id ),
	count(distinct case when ss_rev = 0 or ss_rev is null then provider_id end)
from(
 select 
 tst.Test_group , 
 tst.provider_id ,
 sum(adf.ss_take_revenue) ss_rev
 
  from jc_pro_iteration a
  join jk_basic_10_pros tst on  tst.provider_id = a.provider_id and a.signup_dt = tst.event_date
  left join appointments_details_fact adf
    on adf.provider_id = a.provider_id
      and adf.start between a.signup_dt and a.signup_dt + interval '7 day'
group by 1,2
)a
group by 1

*/

/*
select * 
from appointments_details_fact adf
where 
 adf.start >current_date -30
limit 50 

*/
 
 select 
 tst.Test_group , 
 tst.provider_id ,
 a.payments_14d payments ,
 chose_scheduling_plan_7d,
 coalesce(a.active_7d, 0) active,
 coalesce(a.active_client_booking_7d,0) active_booking,
 coalesce(a.active_ncc_booking_7d,0) active_ncc,
 coalesce(a.p1c_7d,0) p1c,
 coalesce(count(distinct appointment_id),0) apt,
 coalesce(count(distinct case when appt_state = 'completed' then  appointment_id end ) ,0) comp_apt,
 coalesce(sum(adf.ss_take_revenue),0) ss_rev,
 coalesce(sum(adf.pro_net_revenue),0) pro_net,
 coalesce(sum(is_ncd), 0) cnt_ncd,
 coalesce(sum( case when appt_state = 'completed' then is_ncd end) ,0) cnt_cmp_ncd
-- select *
  from jc_pro_iteration a
  join jk_basic_10_pros tst on  tst.provider_id = a.provider_id and a.signup_dt = tst.event_date
  left join appointments_details_fact adf
    on adf.provider_id = a.provider_id
      and adf.start between a.signup_dt and a.signup_dt + interval '14 day'

group by 1,2,3,4,5,6,7,8
 
 



-----  -----  -----  -----

select * 
from appointments_details_fact
where provider_id = '' -- '2158613'--'2157784'

--  --  --  --  --  --  --



-- baseline for Premium pros Paying after trial ends 


SELECT  -- *, case when first_paid_invoice < signup_dt +  interval '60 day' then a.provider_id end 
	DATE_PART(month,signup_dt) , count(distinct a.provider_id) prime, count(distinct case when first_paid_invoice < signup_dt +  interval '60 day' then a.provider_id end ) payers
FROM(select
    pd.id provider_id
    , date(pd.creation_time) signup_dt
    , disabled_payments
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '30 day' then 1 else 0 end p1c_30d
    , case when pd.plan = 'commission' then 'Basic' else 'Premium' end as plan -- is this right
from public.provider_dim pd
where pd.creation_time between  '2023-01-01' and '2023-07-31'
    and pd.type = 1
    and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
    and pd.status <>'staff'
    and exclude_from_reporting <>1

)a
left join(
select provider_id, min(sif.subscription_invoice_paid_date) first_paid_invoice
from subscriptions_invoice_fact sif
join provider_dim pd on pd.id = sif.provider_id
where subscription_invoice_paid_date >= '2023-01-01'
    and is_refunded = 0
    and stripe_charge_status = 1
    and invoice_paid_status = 1
    and pd.type = 1
	and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
	and pd.status <>'staff'
	and pd.exclude_from_reporting <>1
group by 1
)as b ON a.provider_id = b.provider_id
where plan = 'Premium'
group by 1
-- limit 199



---------------------

-- test free to pro convertion

with 
jk_basic_10_pros as (
select
	provider_id
	, test_group
    , event_date :: date 
    -- , row_number() over (partition by provider_id order by event_date asc) event_num
from styleseat_events.styleseat_events_master
where event = 'client_ab_test_assignment'
	and test_name = 'test_pro_basic_10_pricing_fly4076_230825'
    and event_date >= '2023-09-01'
QUALIFY  row_number() over (partition by provider_id order by event_date asc) =1

 )



 
 SELECT  -- *, case when first_paid_invoice < signup_dt +  interval '60 day' then a.provider_id end 
	-- DATE_PART(month,signup_dt) 
 test_group 
	, count(distinct a.provider_id) prime, count(distinct case when first_paid_invoice < signup_dt +  interval '60 day' then a.provider_id end ) payers
FROM(select
    pd.id provider_id
    , date(pd.creation_time) signup_dt
    , disabled_payments
    , case when pd.profile_phase_one_complete_date between pd.creation_time and pd.creation_time + interval '30 day' then 1 else 0 end p1c_30d
    , case when pd.plan = 'commission' then 'Basic' else 'Premium' end as plan -- is this right
from public.provider_dim pd
where pd.creation_time >= '2023-07-31'-- between  '2023-01-01' and '2023-07-31'
    and pd.type = 1
    and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
    and pd.status <>'staff'
    and exclude_from_reporting <>1

)a
left join(
select provider_id, min(sif.subscription_invoice_paid_date) first_paid_invoice
from subscriptions_invoice_fact sif
join provider_dim pd on pd.id = sif.provider_id
where subscription_invoice_paid_date >= '2023-01-01'
    and is_refunded = 0
    and stripe_charge_status = 1
    and invoice_paid_status = 1
    and pd.type = 1
	and pd.email not like '%@tfbnw.net' and pd.email not like '%@styleseat.com'
	and pd.status <>'staff'
	and pd.exclude_from_reporting <>1
group by 1
)as b ON a.provider_id = b.provider_id
join jk_basic_10_pros test on test.provider_id = a.provider_id
where plan = 'Premium'
group by 1







