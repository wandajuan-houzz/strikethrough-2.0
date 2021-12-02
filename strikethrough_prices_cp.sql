   with cp_prices as
    (
        select *, c_min_total_price - c_min_no_ship_price as c_min_shipping
        from wandajuan.min_prices_cp_us_tmp
        where date_diff('day', cast(dt as date), date '2021-11-29') <= 85
--        and house_id = 56312725
    )
    ,

    vld as
    (
        select vendor_listing_id
        , house_id
        , seller_type
        , final_price as price
        , dc
        , preferred_vendor_listing_id
        from pricing.vendor_listings_basic_signals
        where dt = '2021-11-29'
        -- ensure valid listings
        and listing_type = 1
        and parentage in (0, 2)
        and status = 0
        and site_id = 101
        and final_price is not null
    )
    ,

    all_prices as
    (
        select cp_prices.*
        , avg(c_min_no_ship_price) over (partition by cp_prices.house_id) as avg_no_ship_price
        , stddev(c_min_no_ship_price) over (partition by cp_prices.house_id) as std_no_ship_price
        , avg(c_min_total_price) over (partition by cp_prices.house_id) as avg_tot_price
        , stddev(c_min_total_price) over (partition by cp_prices.house_id) as std_tot_price
        , avg(c_min_shipping) over (partition by cp_prices.house_id) as avg_shipping
        , stddev(c_min_shipping) over (partition by cp_prices.house_id) as std_shipping
        from cp_prices
        join
        (
            select house_id, price
            from vld
            where vendor_listing_id = preferred_vendor_listing_id
        ) v
        on cp_prices.house_id = v.house_id
        where c_min_no_ship_price <= 5 * price
        and c_min_shipping <= 2 * c_min_no_ship_price   ---
    )
    ,

    max_prices as
    (
        select all_prices.house_id
        , max(max_no_ship_price) as max_no_ship_price
        , cast(date_add('day', -4, max(if(c_min_no_ship_price = max_no_ship_price, cast(dt as date), null))) as varchar) as no_ship_start_date
        , max(max_total_price) as max_total_price
        , cast(date_add('day', -4, max(if(c_min_total_price = max_total_price, cast(dt as date), null))) as varchar) as total_start_date
        from all_prices
        join
        (
            select house_id
            , max(c_min_no_ship_price) as max_no_ship_price
            , max(c_min_total_price) as max_total_price
            from all_prices
            group by house_id
        ) max_price
        on all_prices.house_id = max_price.house_id
        and (all_prices.c_min_no_ship_price = max_price.max_no_ship_price
        or all_prices.c_min_total_price = max_price.max_total_price)
        where c_min_no_ship_price < avg_no_ship_price + 3.00 * std_no_ship_price
        and c_min_total_price < avg_tot_price + 3.00 * std_tot_price
        group by all_prices.house_id
    )

    select vendor_listing_id
    , vld.house_id
    , case when seller_type <= 0 then max_no_ship_price
           when price > 49 then max_total_price
           when price < 10 then max_total_price - 2.99
           when price < 30 then max_total_price - 3.99
           else max_total_price - 5.99
          end as strikethrough_price
    , if(seller_type <= 0, no_ship_start_date, total_start_date) as occurrence_date
    from max_prices
    join vld
    on max_prices.house_id = vld.house_id