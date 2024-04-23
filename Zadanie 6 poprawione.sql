with all_ads_data as(
select 
	ad_date, 
	url_parameters, 
	coalesce(spend, 0) as spend,  
	coalesce(impressions, 0) as impressions,  
	coalesce(reach, 0) as reach,  
	coalesce(clicks, 0) as clicks,  
	coalesce(leads, 0) as leads,  
	coalesce(value, 0) as value
from facebook_ads_basic_daily fabd
left join facebook_adset fa using (adset_id)
left join facebook_campaign fc using (campaign_id)
	
union all
	
select 
	ad_date, 
	url_parameters, 
	coalesce(spend, 0), 
	coalesce(impressions, 0), 
	coalesce(reach, 0), 
	coalesce(clicks, 0), 
	coalesce(leads, 0), 
	coalesce(value, 0)
from google_ads_basic_daily gabd
),

all_ads_data_1 as (
select 
date(date_trunc('month', ad_date)) as ad_month,
	case
		when lower(substring(url_parameters, 'utm_campaign=([^\&]+)')) != 'nan' 
		then decode_url_part(lower(substring(url_parameters, 'utm_campaign=([^\&]+)')))
	end as utm_campaign,
sum(spend) as total_spend,
sum(impressions) as total_impressions,
sum(clicks) as total_clicks,
sum(value) as total_value,
case
	when sum(clicks)=0 then 0
	else sum(spend::numeric)/sum(clicks)
end as CPC,
case
	when sum(impressions)=0 then 0
	else sum(spend::numeric)/sum(impressions)*1000
end as CPM,
case
	when sum(impressions)=0 then 0
	else sum(clicks::numeric)/sum(impressions)*100
end as CTR,
case
	when sum(spend)=0 then 0
	else (sum(value::numeric)-sum(spend))/sum(spend)*100
end as ROMI
from
all_ads_data
group by 
ad_month,
utm_campaign
),

all_ads_final as (
select 
*,
lag(CPM) over(partition by utm_campaign order by ad_month) as previous_month_cpm,
lag(CTR) over(partition by utm_campaign order by ad_month) as previous_month_ctr,
lag(ROMI) over(partition by utm_campaign order by ad_month) as previous_month_romi
from 
all_ads_data_1
)

select
*,
case
	when CPM=0 then previous_month_cpm/previous_month_cpm*100
	else (CPM-previous_month_cpm)/previous_month_cpm*100
end as CPM_changes_percent,
case
	when CTR=0 then previous_month_ctr/previous_month_ctr*100
	else (CTR-previous_month_ctr)/previous_month_ctr*100
end as CTR_changes_percent,
case
	when ROMI=0 then previous_month_romi/previous_month_romi*100
	else (ROMI-previous_month_romi)/previous_month_romi*100
end as ROMI_changes_percent
from 
all_ads_final
where 
previous_month_cpm<>0
and previous_month_ctr<>0
and previous_month_romi<>0
order by 
ad_month



