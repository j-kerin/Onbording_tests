
drop table jk_hp_test;

create temporary table jk_hp_test as (
 select *
    from (
	select provider_id, user_id, test_group, event_date, cookie_id
        , row_number() over (partition by provider_id order by event_date asc) event_num
	from styleseat_events.styleseat_events_master
	where event = 'client_ab_test_assignment'
		and test_name = 'client_pro_landing_page_apo4232_20231214'
   		and event_date > '2024-01-28'
    )
 where event_num = 1
 )

 
 
 select event_date:: date, hpt.test_group, count(distinct user_id) 
 from jk_hp_test hpt 
 group by 1,2 
 
 
 
 

 
 ------
 
 -- test user logins  

select  hpt.test_group, count(distinct cookie_id) cookies_loging_in
from (select * from styleseat_events.styleseat_events_master
	where event = 'user_model_login_succeeded' and event_date > '2024-01-28' 
	) lis
join jk_hp_test hpt on hpt.user_id = lis.user_id 
	and lis.event_date > hpt.event_date
	and lis.event_date :: date = hpt.event_date :: date
group by 1




select lis.event_date:: date, hpt.test_group, count(distinct cookie_id)
from (select * from styleseat_events.styleseat_events_master
	where event = 'user_model_login_succeeded' and event_date > '2024-01-28' 
	) lis
join jk_hp_test hpt on hpt.user_id = lis.user_id
	and lis.event_date > hpt.event_date
	and lis.event_date :: date = hpt.event_date :: date
group by 1,2

-----







select *
from jk_hp_test hpt

left join (select cookie_id, event_date::date  as event_date
		, max(case when event in ('Pro Onboarding Sign Up Page Viewed','P Viewed Signup Sell Page') then 1 else 0 end ) signup_view
		, max(case when event = 'PS Completed' then 1 else 0 end )as signup_comp
	from styleseat_events.styleseat_events_master
	where event_date >= '2024-01-28' 
 	   and event in (
  	      'Pro Onboarding Sign Up Page Viewed' -- signup-page
 	       , 'P Viewed Signup Sell Page' -- signup-page
  	      , 'PS Completed' ) 
    group by 1,2
  )	 sup on     sup.cookie_id = hpt.cookie_id  and sup.event_date =  hpt.event_date::date
  where sup.cookie_id is not null
limit 50


--------------------------------------


select test_group, count(distinct hpt.cookie_id), count(distinct sup.cookie_id), count(distinct case when signup_comp = 1 then hpt.cookie_id end )
-- select *

from jk_hp_test hpt
-- left join ()

left join (select cookie_id, event_date::date  as event_date
		, max(case when event in ('Pro Onboarding Sign Up Page Viewed','P Viewed Signup Sell Page') then 1 else 0 end ) signup_view
		, max(case when event = 'PS Completed' then 1 else 0 end )as signup_comp
	from styleseat_events.styleseat_events_master
	where event_date >= '2024-01-28' 
 	   and event in (
  	      'Pro Onboarding Sign Up Page Viewed' -- signup-page
 	       , 'P Viewed Signup Sell Page' -- signup-page
  	      , 'PS Completed' ) 
    group by 1,2
  )	 sup on     sup.cookie_id = hpt.cookie_id  and sup.event_date =  hpt.event_date::date
  -- where sup.cookie_id is not null
  group by 1
  
  
  
  -----------------------------------
  	--, utm_source, utm_medium, utm_campaign, platform
	--, 'homepage set up business'as page_type -- json_extract_path_text(context, 'origin', TRUE) origin_text -- context,
 
select *
from (select   cookie_id,  event_date ,event_date:: date as edate , m.event
	, row_number() over (partition by cookie_id order by event_date asc) event_num
from styleseat_events.styleseat_events_master m 
where  m.event like ('provider_homepage_set_up_my_business_link_click')
	and m.event_date >=  '2024-01-28' 
)sub_b

where event_num =1 

limit 50 

-----------------


select test_group, count(distinct hpt.cookie_id), count(distinct sup.cookie_id), count(distinct case when signup_comp = 1 then hpt.cookie_id end )
-- select *

from jk_hp_test hpt
left join (select *
	from (select   cookie_id,  event_date ,event_date:: date as edate , m.event
		, row_number() over (partition by cookie_id order by event_date asc) event_num
	from styleseat_events.styleseat_events_master m 
	where  m.event like ('provider_homepage_set_up_my_business_link_click')
		and m.event_date >=  '2024-01-28' 
	)sub_b
where event_num =1 )hpc 

left join (select cookie_id, event_date::date  as event_date
		, max(case when event in ('Pro Onboarding Sign Up Page Viewed','P Viewed Signup Sell Page') then 1 else 0 end ) signup_view
		, max(case when event = 'PS Completed' then 1 else 0 end )as signup_comp
	from styleseat_events.styleseat_events_master
	where event_date >= '2024-01-28' 
 	   and event in (
  	      'Pro Onboarding Sign Up Page Viewed' -- signup-page
 	       , 'P Viewed Signup Sell Page' -- signup-page
  	      , 'PS Completed' ) 
    group by 1,2
  )	 sup on     sup.cookie_id = hpt.cookie_id  and sup.event_date =  hpt.event_date::date
  -- where sup.cookie_id is not null
  group by 1


