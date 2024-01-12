

-- users viewing the banner and when 

select  user_id, Provider_id,  
	count(distinct  m.event_date:: date  ), max(m.event_date), min(m.event_date)
from styleseat_events.styleseat_events_master m 
where m.event like ('Pro Trial Banner Viewed') 
	and m.event_date >= '2023-09-25'
group by 1,2
limit 50;


-- users clicking the banner and when 
select user_id, Provider_id,  
	count(distinct  m.event_date:: date  ), max(m.event_date), min(m.event_date)
from styleseat_events.styleseat_events_master m 
where m.event like ('Pro Trial Banner CTA Button Clicked') 
	and m.event_date >= '2023-09-25'
group by 1,2 
limit 50;


--  



--view and click banner togeather
create temp table jk_banner as 

select vwd.platform, vwd.user_id , vwd.provider_id, vwd.dvb days_viewing_banner, vwd.last_view , vwd.first_view, clk.clks , last_click, first_click
from (select  user_id, Provider_id,  
		CASE
         WHEN platform = 'app' AND user_agent LIKE '%iPhone%' THEN 'app_iphone'
         WHEN platform = 'app' AND user_agent LIKE '%Android%' THEN 'app_android'
         WHEN platform = 'web' AND responsive_layout = 'mobile' AND user_agent LIKE '%iPhone%' THEN 'mobile_web_iphone'
         WHEN platform = 'web' AND responsive_layout = 'mobile' AND user_agent LIKE '%Android%' THEN 'mobile_web_android'
         WHEN platform = 'web' AND responsive_layout = 'desktop' THEN 'desktop_web'
         ELSE 'others'
       END AS platform,
		count(distinct  m.event_date:: date  ) dvb, max(m.event_date) last_view, min(m.event_date) first_view
	from styleseat_events.styleseat_events_master m 
	where m.event like ('Pro Trial Banner Viewed') 
		and m.event_date >= '2023-09-25'
	group by 1,2,3 )vwd
left join (select user_id, Provider_id, 
		CASE
         WHEN platform = 'app' AND user_agent LIKE '%iPhone%' THEN 'app_iphone'
         WHEN platform = 'app' AND user_agent LIKE '%Android%' THEN 'app_android'
         WHEN platform = 'web' AND responsive_layout = 'mobile' AND user_agent LIKE '%iPhone%' THEN 'mobile_web_iphone'
         WHEN platform = 'web' AND responsive_layout = 'mobile' AND user_agent LIKE '%Android%' THEN 'mobile_web_android'
         WHEN platform = 'web' AND responsive_layout = 'desktop' THEN 'desktop_web'
         ELSE 'others'
       END AS platform, 
		count(distinct  m.event_date:: date  ) clks, max(m.event_date) last_click, min(m.event_date) first_click
	from styleseat_events.styleseat_events_master m 
	where m.event like ('Pro Trial Banner CTA Button Clicked') 
		and m.event_date >= '2023-09-25'
	group by 1,2,3 )clk on vwd.user_id = clk.user_id  and  vwd.Provider_id= clk.Provider_id and vwd.platform = clk.platform

-- limit 50;






create temp table jk_user_sub_paid  as 
select
    sif.provider_id, core_user_id 
    , min( sif.subscription_invoice_paid_date) :: date  first_sub_invoice_paid_date -- as sub_invoice_paid_date
 --    , sum(subscription_amount/100) as subscription_revenue
from subscriptions_invoice_fact sif
join (select id,  core_user_id  from provider_dim where  creation_time >= '2021-01-01') pd on sif.provider_id = pd.id
where subscription_invoice_paid_date >= '2021-01-01'
    and last_day(subscription_invoice_paid_date) < current_date
    and is_refunded = 0
    and stripe_charge_status = 1
    and invoice_paid_status = 1
group by 1,2
-- limit 190


select * 
from  provider_dim
limit 50 

-----------------------
-----------------------
/*
select * 
FROM provider_provider p
JOIN public.auth_user a ON (p.core_user_id = a.id)
JOIN public.provider_providersignupsettings ppss ON (p.id = ppss.provider_id AND p.type=1)
JOIN public.pay_stripecustomer psc ON (psc.user_id = a.id)
JOIN public.pay_stripesubscription pss ON (psc.id = pss.customer_id)
limit 50;
*/

-----------------------
-----------------------


create temp table jk_user_first_cc as 

select stripe_account_id , user_id ,creation_time 
from public.pay_stripeinstrument_dim
where user_id is not null 
Qualify row_number() over (partition by user_id order by creation_time  asc) =1


drop table jk_provider

create temp table jk_provider as 
select pd.id as provider_id, core_user_id , creation_time 
	, case when pd.profile_phase_one_complete_date is not null then 1 else 0 end as p1c_flag
	, case when pcd.plan = 'deluxe' then 'Premium' else 'Basic' end as plan_7d 
	, case when pd.plan = 'deluxe' then 'Premium' else 'Basic' end as plan_current  
	, case when pcd2.provider_id is not null then 1 else 0 end  as premium_ever_flag
	, Premium_start_date
from provider_dim pd
left join ( select * from  provider_change_dim
		) pcd on pd.id = pcd.provider_id
		and pd.creation_time + interval '7 day' between pcd.effective_start_date and pcd.effective_end_date
left join ( select provider_id , min(effective_start_date) Premium_start_date from  provider_change_dim where plan = 'deluxe' group by 1
		) pcd2 on pd.id = pcd2.provider_id
where creation_time >= '2023-01-01'
	and p1c_flag <> 0 
and email not like '%@tfbnw.net' and email not like '%@styleseat.com' 
and exclude_from_reporting <> 1 and status <> 'staff' 



select distinct provider_id  from  provider_change_dim where plan = 'deluxe'
limit 50;
 
 case when pla
 

 jk_banner
 jk_user_sub_paid

 
 
 
 select *
 from jk_provider pro 
 left join jk_banner ban on ban.provider_id = pro.provider_id
 left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
 left join jk_user_first_cc cc on cc.user_id = pro.core_user_id
 where plan_7d = 'Premium' or plan_current = 'Premium'
 limit 199
 
 
 
 select last_day(pro.creation_time:: date), count(1) 
  from jk_provider pro 
 left join jk_banner ban on ban.provider_id = pro.provider_id
 left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
 left join jk_user_first_cc cc on cc.user_id = pro.core_user_id
 where ban.user_id is not null 
 group by 1 
 order by 1
-----  -----  -----  -----  -----  -----  -----  
 
 select last_day(pro.creation_time:: date), count(distinct pro.provider_id) pros
 	, count( distinct case when  plan_7d = 'Premium' or plan_current = 'Premium' then pro.provider_id end) premium_pros_7d
 	, count( distinct case when  premium_ever_flag then pro.provider_id end) premium_ever_pro
 	, count( distinct case when  Premium_start_date >= '2023-09-01' then pro.provider_id end) Premium_after_9_1
 	
 	, count( distinct case when  ban.user_id is not null  then pro.provider_id end)  pvb
 	, sum(days_viewing_banner) banner_views
 	, count( distinct case when  ban.clks is not null  then pro.provider_id end)  pros_clicking_banner 
 	, sum(clks) clks
 	, count( distinct case when usp.provider_id is not null  then pro.provider_id end)  sub_paid
 	, count( distinct case when ban.first_view <  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_abv 
  	, count( distinct case when ban.first_click <  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_abc
  	, count( distinct case when  Premium_start_date + interval '35 day' >=  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_5at 

  	
 	, count( distinct case when  cc.user_id is not null  then pro.provider_id end)  added_cc
 	, count( distinct case when ban.first_view < cc.creation_time  then pro.provider_id end)  added_cc_abv  
 	, count( distinct case when ban.first_click < cc.creation_time  then pro.provider_id end)  added_cc_abc  
 from jk_provider pro 
 left join jk_banner ban on ban.provider_id = pro.provider_id
 left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
 left join jk_user_first_cc cc on cc.user_id = pro.core_user_id
 -- where ban.user_id is not null 
 group by 1 
 order by 1
 
 first_sub_invoice_paid_date
 
 
 
 -----  -----  -----  -----  -----  -----  -----  
 
 select  case when ban.user_id is not null  then 1 else 0 end saw_banner_flag
 	, case when platform = 'app_iphone' then 1 else 0 end ios_app
 --	, case when ban.first_view < cc.creation_time   then 1 else 0 end cc_after_view
 	, count(distinct pro.provider_id) pro
 	, count( distinct case when  Premium_start_date + interval '35 day' >=  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_5at 
 
from jk_provider pro 
left join jk_banner ban on ban.provider_id = pro.provider_id
left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
--left join jk_user_first_cc cc on cc.user_id = pro.core_user_id
where premium_ever_flag = 1 
	and pro.creation_time < current_date - interval '36 day'  -- not between '2023-10-02' and '2023-10-20'
group by 1,2
 


select * from jk_banner limit 50 ;

select  both_groups , count(1)
from (select provider_id , count(distinct platform) , max(case when platform = 'app_iphone' then 1 else 0 end) as ios_app_flag
	, max(case when platform <> 'app_iphone' then 1 else 0 end) as non_ios_app_flag
	, non_ios_app_flag + ios_app_flag as both_groups
from jk_banner 
group by 1 
)a
group by 1

--
select  last_day(Premium_start_date:: date) ,case when Premium_start_date >= '2023-09-26'::date then 1 else 0 end, count(distinct pro.provider_id )
, count( distinct case when  Premium_start_date + interval '35 day' >=  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_5at 
from jk_provider pro 
--left join jk_banner ban on ban.provider_id = pro.provider_id
left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
where premium_ever_flag = 1 
	and pro.creation_time < current_date - interval '36 day' 
group by 1,2	
order by 1,2


--  
/*
select  case when ban.clks is not null then 1 else 0 end saw_banner_flag
 	, count(distinct pro.provider_id) pro
 	, count( distinct case when  Premium_start_date + interval '35 day' >=  first_sub_invoice_paid_date  then pro.provider_id end)  sub_paid_5at 
 
from jk_provider pro 
left join jk_banner ban on ban.provider_id = pro.provider_id
left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
left join jk_user_first_cc cc on cc.user_id = pro.core_user_id
where premium_ever_flag = 1 
--	and pro.creation_time not between '2023-10-02' and '2023-10-20'
group by 1
*/
	

select * 

from jk_provider pro 
left join jk_banner ban on ban.provider_id = pro.provider_id
left join jk_user_sub_paid usp on usp.provider_id = pro.provider_id
where premium_ever_flag = 1 
	and pro.creation_time < current_date - interval '36 day' 
