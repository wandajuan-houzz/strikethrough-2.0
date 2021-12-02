-- current discount histogram
with t as (
select 
		cast(json_extract_scalar(variation_attr, '$.strp_price') as double) strp_price,
		json_extract_scalar(variation_attr, '$.strp_price_type') strp_price_type,		
		json_extract_scalar(variation_attr, '$.strp_price_t') strp_price_test,
		json_extract_scalar(variation_attr, '$.strp_price_type_t') strp_price_type_test,
		json_extract_scalar(variation_attr, '$.strp_start_date') strp_start_date,	
		cast(json_extract_scalar(variation_attr, '$.fp') as double) fp,
		vl.*,
        case
            when promo.vendor_listing_id is not null or pcg.vendor_id is not null then coalesce(least(cast(json_extract_scalar(variation_attr, '$.fp') as double),vl.price),vl.price)
            else vl.price
        end as final_display_price,

        -- ** modify with new strikethrough price & source
		csv.strikethrough_price, csv.source
from c2.vendor_listings vl
join c2.vendor_info_daily vi
on vl.vendor_id = vi.user_id
join c2.houses h
on vl.house_id = h.house_id 
join
    (
    SELECT
        P.house_id,
        max(p.preferred_vendor_listing_id) as preferred_vendor_listing_id,
        COALESCE(AVG(R.review_score)/10, AVG(P.review_score)/10) AS rating,
        CAST(COALESCE(AVG(R.num_reviews), AVG(P.num_reviews)) AS INT) AS num_reviews
    FROM
        c2.product_info P
        INNER JOIN
        c2.product_attributes T
        ON P.house_id = T.house_id
        LEFT OUTER JOIN c2.product_info R
        ON T.parent_product_id = R.house_id
    where p.site_id = 101
    GROUP BY P.house_id
    ) as pi
on vl.house_id = pi.house_id
left join
(
    select
        vendor_listing_id,
        max(map_override) as map_override,
        case when min(if(map_override = 1, map_override_percent_value, null)) = 0 then 100
             else max(map_override_percent_value) end as map_override_percent_value
    from
        (select * from c2.promotion_campaigns where status=1
        and '2021-11-22' between start_date and end_date) pc
        inner join
        c2.vendor_listing_group_references vlgr
        on pc.group_id = vlgr.group_id
    group by vendor_listing_id
) promo
on vl.vendor_listing_id = promo.vendor_listing_id
left join
(
    select
        vendor_id,
        max(map_override) as map_override,
        case when min(if(map_override = 1, map_override_percent_value, null)) = 0 then 100
             else max(map_override_percent_value) end as map_override_percent_value
    from
        c2.promotion_campaigns
    where status=1
    and '2021-11-22' between start_date and end_date
    and group_id = 1
    group by vendor_id
) pcg
on vl.vendor_id = pcg.vendor_id
-- ** join with table to generate new strp
left join wandajuan.strikethrough_prices_v2_1129 csv
on vl.vendor_listing_id = csv.vendor_listing_id 
--and csv.dt = '2021-11-21'
--and csv.dt = '2021-11-22'
where 
vl.listing_type = 1
and vl.parentage in (0, 2)
and vi.site_id = 101
and vl.vendor_listing_id = pi.preferred_vendor_listing_id
-- all status valid
and vl.status = 0
and vi.status = 1
and h.status = 0
)
select 	
		if(final_display_price < strp_price, ceil(10*final_display_price/strp_price-10)*0.1, null) fdp_per_discount,	
		count(*) n_products
from t
group by 1
order by 1


-- new discount histogram

with t as (
select 
		cast(json_extract_scalar(variation_attr, '$.strp_price') as double) strp_price,
		json_extract_scalar(variation_attr, '$.strp_price_type') strp_price_type,		
		json_extract_scalar(variation_attr, '$.strp_price_t') strp_price_test,
		json_extract_scalar(variation_attr, '$.strp_price_type_t') strp_price_type_test,
		json_extract_scalar(variation_attr, '$.strp_start_date') strp_start_date,	
		cast(json_extract_scalar(variation_attr, '$.fp') as double) fp,
		vl.*,
        case
            when promo.vendor_listing_id is not null or pcg.vendor_id is not null then coalesce(least(cast(json_extract_scalar(variation_attr, '$.fp') as double),vl.price),vl.price)
            else vl.price
        end as final_display_price,

        -- ** modify with new strikethrough price & source
		csv.strikethrough_price, csv.source
from c2.vendor_listings vl
join c2.vendor_info_daily vi
on vl.vendor_id = vi.user_id
join c2.houses h
on vl.house_id = h.house_id 
join
    (
    SELECT
        P.house_id,
        max(p.preferred_vendor_listing_id) as preferred_vendor_listing_id,
        COALESCE(AVG(R.review_score)/10, AVG(P.review_score)/10) AS rating,
        CAST(COALESCE(AVG(R.num_reviews), AVG(P.num_reviews)) AS INT) AS num_reviews
    FROM
        c2.product_info P
        INNER JOIN
        c2.product_attributes T
        ON P.house_id = T.house_id
        LEFT OUTER JOIN c2.product_info R
        ON T.parent_product_id = R.house_id
    where p.site_id = 101
    GROUP BY P.house_id
    ) as pi
on vl.house_id = pi.house_id
-- ** join with table to generate new strp
left join wandajuan.strikethrough_prices_v2_1129 csv
on vl.vendor_listing_id = csv.vendor_listing_id 
--and csv.dt = '2021-11-21'
--and csv.dt = '2021-11-22'
left join
(
    select
        vendor_listing_id,
        max(map_override) as map_override,
        case when min(if(map_override = 1, map_override_percent_value, null)) = 0 then 100
             else max(map_override_percent_value) end as map_override_percent_value
    from
        (select * from c2.promotion_campaigns where status=1
        and '2021-11-22' between start_date and end_date) pc
        inner join
        c2.vendor_listing_group_references vlgr
        on pc.group_id = vlgr.group_id
    group by vendor_listing_id
) promo
on vl.vendor_listing_id = promo.vendor_listing_id
left join
(
    select
        vendor_id,
        max(map_override) as map_override,
        case when min(if(map_override = 1, map_override_percent_value, null)) = 0 then 100
             else max(map_override_percent_value) end as map_override_percent_value
    from
        c2.promotion_campaigns
    where status=1
    and '2021-11-22' between start_date and end_date
    and group_id = 1
    group by vendor_id
) pcg
on vl.vendor_id = pcg.vendor_id
where 
vl.listing_type = 1
and vl.parentage in (0, 2)
and vi.site_id = 101
and vl.vendor_listing_id = pi.preferred_vendor_listing_id
-- all status valid
and vl.status = 0
and vi.status = 1
and h.status = 0
)
select 	
		if(final_display_price < strikethrough_price, ceil(10*final_display_price/strikethrough_price-10)*0.1, null) fdp_per_discount,
		count(*) n_products
from t
group by 1
order by 1