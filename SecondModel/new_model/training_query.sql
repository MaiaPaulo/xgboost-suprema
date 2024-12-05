WITH counts AS (
    SELECT
        start_date_local,
        start_hour_local,
        COUNT(*) AS same_hour_game
    FROM
        bi_mtt_overlay
    WHERE
        match_type_detail_name = 'MTT-NLH'
        AND class_match IN ('Micro', 'Low')
    GROUP BY
        start_date_local,
        start_hour_local
),
one_hour_window_counts AS (
    SELECT
        c1.start_date_local,
        c1.start_hour_local,
        SUM(c2.same_hour_game) AS one_hour_window_game
    FROM
        counts c1
    LEFT JOIN
        counts c2 ON c1.start_date_local = c2.start_date_local
        AND c2.start_hour_local BETWEEN c1.start_hour_local - 1 AND c1.start_hour_local + 1
    GROUP BY
        c1.start_date_local,
        c1.start_hour_local
)
SELECT
    t1.match_id,
    t1.start_date_local,
    t1.start_hour_local,
    t1.day_of_week_local,
    t1.ko_type,
    t1.mtt_pool_allocation_description,
    CAST(t1.level_reject AS INTEGER) AS level_reject,
    t1.estrutura_blinds,
    t1.mtt_blind_up_time_sec,
    t1.late_registration_time,
    t1.earlybird,
    t1.has_rebuy,
    t1.has_addon,
    t1.mtt_customer_buy_in,
    t1.mtt_customer_rebuy,
    t1.mtt_customer_addon,
    t1.gtd,
    t1.has_overlay,
    t1.overlay,
    t1.mtt_result,
    t1.collected,
    t1.normalized_mtt_result,
    t1.normalized_collected,
    t1.Buyin_Gtd_rate,
    c.same_hour_game,
    o.one_hour_window_game
FROM
    bi_mtt_overlay t1
LEFT JOIN counts c ON
    t1.start_date_local = c.start_date_local
    AND t1.start_hour_local = c.start_hour_local
LEFT JOIN one_hour_window_counts o ON
    t1.start_date_local = o.start_date_local
    AND t1.start_hour_local = o.start_hour_local
WHERE
    t1.match_type_detail_name = 'MTT-NLH'
    AND t1.class_match IN ('Micro', 'Low')
    AND t1.start_date_local between '2023-01-01' AND '2024-12-31'