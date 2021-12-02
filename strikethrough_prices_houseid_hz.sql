WITH
    all_prices AS (
        
        
        select *
        , 101 as site_id,
        avg(c_min_price) over (partition by vendor_listing_id) as avg_price,   ----<<
        stddev(c_min_price) over (partition by vendor_listing_id) as std_price  ----<<
        from marketplace.min_prices_hz_us
        where `min_prices_hz_us`.dt BETWEEN "2021-08-29" AND "2021-11-21"
    )
    ,

    vlid_hid_level AS (
        select vlid_level.vendor_listing_id
        , strikethrough_price
        , house_id
        , all_prices.site_id
        , cast(date_sub(max(dt), 4) as string) as occurrence_date
        from all_prices
        join
        (
            select vendor_listing_id
            , max(c_min_price) as strikethrough_price
            from all_prices
            where c_min_price < avg_price + 3.00 * std_price    -- <<<<<<<<<<< a. remove outliers
            group by vendor_listing_id

        ) vlid_level
        on all_prices.vendor_listing_id = vlid_level.vendor_listing_id
        and all_prices.c_min_price = vlid_level.strikethrough_price
        left join c2.vendor_listings master
        on all_prices.vendor_listing_id = master.vendor_listing_id
        where                                    
        master.listing_type = 1                    -- <<<<<<<<<<< b. remove invalid referrenced sibling vlid
        and master.parentage in (0, 2)
        and master.status = 0                     -- <<<<<<<<<<<
        group by vlid_level.vendor_listing_id, strikethrough_price, house_id, all_prices.site_id
    )

select vendor_listing_id
, vlid_hid_level.house_id
, coalesce(max_price, strikethrough_price) as strikethrough_price
, vlid_hid_level.site_id
, coalesce(max_occurrence_date, occurrence_date) as occurrence_date
from vlid_hid_level
left join
(
    select house_id
    , site_id
    , max(strikethrough_price) as max_price
    , max(occurrence_date) as max_occurrence_date
    from vlid_hid_level
    group by house_id, site_id
) hid_level
on vlid_hid_level.house_id = hid_level.house_id and vlid_hid_level.site_id = hid_level.site_id
;