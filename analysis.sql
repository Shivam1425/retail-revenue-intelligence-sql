
/* =========================================================
PROJECT: Retail Demand and Revenue Intelligence
AUTHOR: Shivam Kumar
PURPOSE: Advanced MySQL 8 analysis for revenue diagnostics, demand planning,
         store productivity, promotion economics, and event sensitivity.
DIALECT: MySQL 8.0
========================================================= */

-- ======================================================================
-- RETAIL DEMAND & REVENUE INTELLIGENCE
-- Design standard for this file:
-- 1. Build reusable analytical helper tables first.
-- 2. Use comparable-store logic instead of naive topline comparisons.
-- 3. Attribute holidays by National / Regional / Local applicability.
-- 4. Combine revenue with transactions, promo mix, and external context.
-- 5. Keep every query decision-oriented and portfolio-ready.
-- ======================================================================


/* =========================================================
SECTION 0: ANALYTICAL FOUNDATION TABLES
========================================================= */

DROP VIEW IF EXISTS vw_store_day_metrics;

-- Design Note: Aggregates raw family-level sales to the store-day grain.
-- This serves as the primary fact table for all downstream time-series analysis.
CREATE OR REPLACE VIEW vw_store_day_metrics AS
SELECT
    t.date,
    DATE_SUB(t.date, INTERVAL DAYOFMONTH(t.date) - 1 DAY) AS month_start,
    YEAR(t.date) AS year_num,
    MONTH(t.date) AS month_num,
    MONTHNAME(t.date) AS month_name,
    WEEKDAY(t.date) AS weekday_num,
    DAYNAME(t.date) AS weekday_name,
    CASE WHEN WEEKDAY(t.date) IN (5, 6) THEN 1 ELSE 0 END AS is_weekend,
    t.store_nbr,
    st.city,
    st.state,
    st.type AS store_type,
    st.cluster,
    SUM(t.sales) AS total_sales,
    SUM(COALESCE(t.onpromotion, 0)) AS total_items_on_promo,
    SUM(CASE WHEN COALESCE(t.onpromotion, 0) > 0 THEN 1 ELSE 0 END) AS promoted_family_count,
    COUNT(*) AS family_rows
FROM train t
JOIN stores st
    ON t.store_nbr = st.store_nbr
GROUP BY
    t.date,
    month_start,
    year_num,
    month_num,
    month_name,
    weekday_num,
    weekday_name,
    is_weekend,
    t.store_nbr,
    st.city,
    st.state,
    store_type,
    st.cluster;




DROP VIEW IF EXISTS vw_oil_prices_filled;

-- Design Note: Solves the "sparse data" problem. Oil prices are weekday-only;
-- we use the COUNT window trick to 'carry' the last known price into weekends.
CREATE OR REPLACE VIEW vw_oil_prices_filled AS
WITH oil_groups AS (
    SELECT 
        date, 
        dcoilwtico,
        COUNT(dcoilwtico) OVER (ORDER BY date) as oil_group
    FROM oil
)
SELECT 
    date,
    FIRST_VALUE(dcoilwtico) OVER (PARTITION BY oil_group ORDER BY date) as oil_price_filled
FROM oil_groups;




DROP VIEW IF EXISTS vw_store_day_enriched;

CREATE OR REPLACE VIEW vw_store_day_enriched AS
SELECT
    m.*,
    COALESCE(tr.transactions, 0) AS transactions,
    CASE WHEN tr.transactions IS NULL THEN 1 ELSE 0 END AS missing_transactions_flag,
    o.oil_price_filled AS oil_price,
    CASE WHEN o.oil_price_filled IS NULL THEN 1 ELSE 0 END AS missing_oil_flag,
    ROUND(100.0 * m.promoted_family_count / NULLIF(m.family_rows, 0), 2) AS promo_family_mix_pct,
    CASE
        WHEN COALESCE(tr.transactions, 0) > 0
        THEN m.total_sales / tr.transactions
        ELSE NULL
    END AS sales_per_transaction
FROM vw_store_day_metrics m
LEFT JOIN transactions tr
    ON m.date = tr.date
   AND m.store_nbr = tr.store_nbr
LEFT JOIN vw_oil_prices_filled o
    ON m.date = o.date;




DROP VIEW IF EXISTS vw_store_holiday_map;

-- Design Note: Prevents "data leakage" by matching holidays to stores only where 
-- they apply (e.g. city holidays don't affect stores in other cities).
CREATE OR REPLACE VIEW vw_store_holiday_map AS
SELECT
    h.date,
    st.store_nbr,
    'National' AS holiday_scope,
    h.type AS event_type,
    h.description
FROM holidays_events h
JOIN stores st
    ON 1 = 1
WHERE h.locale = 'National'
  AND h.type IN ('Holiday', 'Additional', 'Bridge', 'Event', 'Transfer')
  AND COALESCE(LOWER(h.transferred), 'false') <> 'true'

UNION ALL

SELECT
    h.date,
    st.store_nbr,
    'Regional' AS holiday_scope,
    h.type AS event_type,
    h.description
FROM holidays_events h
JOIN stores st
    ON h.locale = 'Regional'
   AND st.state = h.locale_name
WHERE h.type IN ('Holiday', 'Additional', 'Bridge', 'Event', 'Transfer')
  AND COALESCE(LOWER(h.transferred), 'false') <> 'true'

UNION ALL

SELECT
    h.date,
    st.store_nbr,
    'Local' AS holiday_scope,
    h.type AS event_type,
    h.description
FROM holidays_events h
JOIN stores st
    ON h.locale = 'Local'
   AND st.city = h.locale_name
WHERE h.type IN ('Holiday', 'Additional', 'Bridge', 'Event', 'Transfer')
  AND COALESCE(LOWER(h.transferred), 'false') <> 'true';




DROP VIEW IF EXISTS vw_store_day_context;

-- Design Note: The 'Universal Analytical Record' that joins metrics, traffic, 
-- macro context, and events into a single, clean flattened row.
CREATE OR REPLACE VIEW vw_store_day_context AS
SELECT
    e.*,
    COALESCE(h.is_special_day, 0) AS is_special_day,
    COALESCE(h.is_holiday, 0) AS is_holiday,
    COALESCE(h.holiday_scope, 'None') AS holiday_scope,
    COALESCE(h.event_type_list, 'None') AS event_type_list,
    COALESCE(h.event_description_list, 'None') AS event_description_list
FROM vw_store_day_enriched e
LEFT JOIN (
    SELECT
        date,
        store_nbr,
        1 AS is_special_day,
        MAX(CASE WHEN event_type = 'Holiday' THEN 1 ELSE 0 END) AS is_holiday,
        CASE
            WHEN COUNT(DISTINCT holiday_scope) > 1 THEN 'Mixed'
            ELSE MIN(holiday_scope)
        END AS holiday_scope,
        GROUP_CONCAT(DISTINCT event_type ORDER BY event_type SEPARATOR ', ') AS event_type_list,
        GROUP_CONCAT(DISTINCT description ORDER BY description SEPARATOR ' | ') AS event_description_list
    FROM vw_store_holiday_map
    GROUP BY date, store_nbr
) h
    ON e.date = h.date
   AND e.store_nbr = h.store_nbr;




/* =========================================================
SECTION 1: DATA GOVERNANCE & READINESS
========================================================= */

-- Q1. What is the analytical coverage quality of the store-day layer?
SELECT
    COUNT(*) AS store_day_rows,
    COUNT(DISTINCT date) AS calendar_days,
    COUNT(DISTINCT store_nbr) AS stores_covered,
    ROUND(SUM(total_sales), 2) AS total_revenue,
    SUM(CASE WHEN missing_transactions_flag = 1 THEN 1 ELSE 0 END) AS missing_transaction_store_days,
    ROUND(100.0 * SUM(CASE WHEN missing_transactions_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_transaction_pct,
    SUM(CASE WHEN missing_oil_flag = 1 THEN 1 ELSE 0 END) AS missing_oil_store_days,
    ROUND(100.0 * SUM(CASE WHEN missing_oil_flag = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_oil_pct,
    SUM(CASE WHEN total_sales = 0 THEN 1 ELSE 0 END) AS zero_sales_store_days
FROM vw_store_day_context;

-- Insight: Mature analysis starts with coverage health, not charts; transaction
-- and oil coverage gaps tell you how much operational and macro analysis can be trusted.
-- Recommendation: Use this query as a release gate before publishing any KPI
-- dashboard or executive commentary built from the dataset.


-- Q2. Which operational anomalies should be flagged before interpretation?
SELECT
    COUNT(*) AS total_store_days,
    SUM(CASE WHEN total_sales < 0 THEN 1 ELSE 0 END) AS negative_revenue_store_days,
    SUM(CASE WHEN total_sales > 0 AND transactions = 0 THEN 1 ELSE 0 END) AS sales_without_transactions_store_days,
    SUM(CASE WHEN total_sales = 0 AND transactions > 0 THEN 1 ELSE 0 END) AS traffic_without_sales_store_days,
    SUM(CASE WHEN promo_family_mix_pct >= 50 AND total_sales = 0 THEN 1 ELSE 0 END) AS heavy_promo_zero_sales_store_days,
    SUM(CASE WHEN total_sales > 0 AND sales_per_transaction IS NULL THEN 1 ELSE 0 END) AS unscorable_basket_quality_store_days
FROM vw_store_day_context;

-- Insight: The highest-risk errors in retail data are not just nulls; they are
-- logical contradictions such as traffic with no sales or sales with no transactions.
-- Recommendation: Route these anomalies into a data-quality exception log so
-- finance, operations, and analytics are working from the same version of truth.


/* =========================================================
SECTION 2: EXECUTIVE KPI & DEMAND TRENDING
========================================================= */

-- Q3. What does the monthly executive KPI dashboard look like?
WITH monthly_kpi AS (
    SELECT
        month_start,
        SUM(total_sales) AS revenue,
        SUM(transactions) AS transactions,
        AVG(total_sales) AS avg_store_day_revenue,
        COUNT(DISTINCT CASE WHEN total_sales > 0 THEN store_nbr END) AS active_stores,
        AVG(promo_family_mix_pct) AS avg_promo_family_mix_pct
    FROM vw_store_day_context
    GROUP BY month_start
)
SELECT
    DATE_FORMAT(month_start, '%Y-%m') AS month,
    ROUND(revenue, 2) AS revenue,
    transactions,
    ROUND(revenue / NULLIF(transactions, 0), 2) AS avg_basket_value,
    ROUND(avg_store_day_revenue, 2) AS avg_store_day_revenue,
    active_stores,
    ROUND(avg_promo_family_mix_pct, 2) AS avg_promo_family_mix_pct,
    ROUND(AVG(revenue) OVER (ORDER BY month_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3m_revenue,
    ROUND(
        100.0 * (revenue - LAG(revenue) OVER (ORDER BY month_start))
        / NULLIF(LAG(revenue) OVER (ORDER BY month_start), 0),
        2
    ) AS mom_growth_pct,
    ROUND(
        100.0 * (revenue - LAG(revenue, 12) OVER (ORDER BY month_start))
        / NULLIF(LAG(revenue, 12) OVER (ORDER BY month_start), 0),
        2
    ) AS yoy_growth_pct
FROM monthly_kpi
ORDER BY month_start;

-- Insight: A strong monthly dashboard should combine scale, traffic, basket,
-- promo mix, and trend, because no single KPI explains revenue on its own.
-- Recommendation: Use this table as the executive front page and investigate
-- months where revenue, transactions, and basket move in conflicting directions.


-- Q4. What is the comparable-store monthly growth trend?
WITH monthly_store AS (
    SELECT
        month_start,
        YEAR(month_start) AS year_num,
        MONTH(month_start) AS month_num,
        store_nbr,
        SUM(total_sales) AS revenue,
        SUM(transactions) AS transactions
    FROM vw_store_day_context
    GROUP BY month_start, store_nbr
),
comparable AS (
    SELECT
        cur.month_start,
        COUNT(*) AS comparable_stores,
        SUM(cur.revenue) AS current_revenue,
        SUM(prev.revenue) AS prior_year_revenue,
        SUM(cur.transactions) AS current_transactions,
        SUM(prev.transactions) AS prior_year_transactions
    FROM monthly_store cur
    JOIN monthly_store prev
        ON cur.store_nbr = prev.store_nbr
       AND cur.month_num = prev.month_num
       AND cur.year_num = prev.year_num + 1
    GROUP BY cur.month_start
)
SELECT
    DATE_FORMAT(month_start, '%Y-%m') AS month,
    comparable_stores,
    ROUND(current_revenue, 2) AS current_revenue,
    ROUND(prior_year_revenue, 2) AS prior_year_revenue,
    ROUND(
        100.0 * (current_revenue - prior_year_revenue) / NULLIF(prior_year_revenue, 0),
        2
    ) AS same_store_sales_growth_pct,
    ROUND(
        100.0 * (current_transactions - prior_year_transactions) / NULLIF(prior_year_transactions, 0),
        2
    ) AS same_store_transaction_growth_pct,
    ROUND(
        100.0 * (
            (current_revenue / NULLIF(current_transactions, 0))
            - (prior_year_revenue / NULLIF(prior_year_transactions, 0))
        ) / NULLIF((prior_year_revenue / NULLIF(prior_year_transactions, 0)), 0),
        2
    ) AS same_store_basket_growth_pct
FROM comparable
ORDER BY month_start;

-- Insight: Same-store analysis is the cleaner operating view because it strips
-- out the noise of network mix changes and focuses on comparable productivity.
-- Recommendation: Use comparable-store growth as the primary health metric for
-- recurring business reviews, with topline revenue as a secondary lens.


-- Q5. How does seasonality behave by month and weekday?
SELECT
    month_num,
    month_name,
    weekday_num + 1 AS weekday_order_monday_start,
    weekday_name,
    ROUND(AVG(total_sales), 2) AS avg_store_day_revenue,
    ROUND(AVG(transactions), 2) AS avg_transactions,
    ROUND(AVG(sales_per_transaction), 2) AS avg_sales_per_transaction
FROM vw_store_day_context
GROUP BY month_num, month_name, weekday_num, weekday_name
ORDER BY month_num, weekday_num;

-- Insight: Strong retail planning depends on seasonal shape, not just annual totals;
-- weekday-month interaction reveals where staffing, replenishment, and campaigns should flex.
-- Recommendation: Use this matrix for staffing plans, delivery cutoffs, and
-- promo timing rather than assuming one seasonality pattern fits the whole year.

/* =========================================================
SECTION 3: CATEGORY PORTFOLIO & PROMOTION ECONOMICS
========================================================= */

-- Q6. Which product families are core growth drivers versus volatile bets?
WITH family_month AS (
    SELECT
        DATE_SUB(date, INTERVAL DAYOFMONTH(date) - 1 DAY) AS month_start,
        family,
        SUM(sales) AS monthly_revenue
    FROM train
    GROUP BY month_start, family
),
latest_month AS (
    SELECT MAX(month_start) AS max_month
    FROM family_month
),
family_revenue AS (
    SELECT
        family,
        SUM(monthly_revenue) AS total_revenue
    FROM family_month
    GROUP BY family
),
family_stability AS (
    SELECT
        family,
        ROUND(STDDEV_POP(monthly_revenue) / NULLIF(AVG(monthly_revenue), 0), 3) AS revenue_cv
    FROM family_month
    GROUP BY family
),
family_growth AS (
    SELECT
        fm.family,
        SUM(
            CASE
                WHEN fm.month_start BETWEEN DATE_SUB(lm.max_month, INTERVAL 5 MONTH) AND lm.max_month
                THEN fm.monthly_revenue
                ELSE 0
            END
        ) AS recent_6m_revenue,
        SUM(
            CASE
                WHEN fm.month_start BETWEEN DATE_SUB(lm.max_month, INTERVAL 11 MONTH) AND DATE_SUB(lm.max_month, INTERVAL 6 MONTH)
                THEN fm.monthly_revenue
                ELSE 0
            END
        ) AS prior_6m_revenue
    FROM family_month fm
    CROSS JOIN latest_month lm
    GROUP BY fm.family, lm.max_month
),
family_metrics AS (
    SELECT
        fr.family,
        fr.total_revenue,
        fs.revenue_cv,
        ROUND(100.0 * (fg.recent_6m_revenue - fg.prior_6m_revenue) / NULLIF(fg.prior_6m_revenue, 0), 2) AS recent_6m_growth_pct,
        PERCENT_RANK() OVER (ORDER BY fr.total_revenue) AS revenue_percentile,
        PERCENT_RANK() OVER (ORDER BY fs.revenue_cv) AS cv_percentile,
        PERCENT_RANK() OVER (ORDER BY ROUND(100.0 * (fg.recent_6m_revenue - fg.prior_6m_revenue) / NULLIF(fg.prior_6m_revenue, 0), 2)) AS growth_percentile
    FROM family_revenue fr
    JOIN family_stability fs ON fr.family = fs.family
    JOIN family_growth fg ON fr.family = fg.family
)
SELECT
    family,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (), 2) AS revenue_share_pct,
    revenue_cv,
    recent_6m_growth_pct,
    CASE
        WHEN revenue_percentile >= 0.80 AND cv_percentile <= 0.50 AND growth_percentile >= 0.80 THEN 'Core Growth Driver'
        WHEN revenue_percentile >= 0.80 AND cv_percentile <= 0.30 THEN 'Stable Cash Generator'
        WHEN cv_percentile >= 0.80 AND growth_percentile >= 0.80 THEN 'Volatile Growth Bet'
        ELSE 'Monitor / Niche'
    END AS portfolio_role
FROM family_metrics
ORDER BY revenue_share_pct DESC, recent_6m_growth_pct DESC;

-- Insight: Category leadership is not just about size; a good portfolio view separates stable cash generators from volatile growth stories and niche bets using percentile rankings rather than absolute thresholds.


-- Q7. Which categories are most promotion-dependent, and where is promo uplift strongest?
WITH family_promo AS (
    SELECT
        family,
        SUM(sales) AS total_revenue,
        ROUND(100.0 * AVG(CASE WHEN COALESCE(onpromotion, 0) > 0 THEN 1 ELSE 0 END), 2) AS promo_row_pct,
        ROUND(AVG(CASE WHEN COALESCE(onpromotion, 0) > 0 THEN sales END), 2) AS avg_sales_when_promoted,
        ROUND(AVG(CASE WHEN COALESCE(onpromotion, 0) = 0 THEN sales END), 2) AS avg_sales_without_promo
    FROM train
    GROUP BY family
)
SELECT
    family,
    ROUND(total_revenue, 2) AS total_revenue,
    promo_row_pct,
    avg_sales_when_promoted,
    avg_sales_without_promo,
    ROUND(
        100.0 * (avg_sales_when_promoted - avg_sales_without_promo)
        / NULLIF(avg_sales_without_promo, 0),
        2
    ) AS promo_uplift_pct,
    CASE
        WHEN promo_row_pct >= 25 AND (
            100.0 * (avg_sales_when_promoted - avg_sales_without_promo)
            / NULLIF(avg_sales_without_promo, 0)
        ) >= 25
        THEN 'High Dependency / High Response'
        WHEN promo_row_pct >= 25
        THEN 'High Dependency / Low Response'
        WHEN (
            100.0 * (avg_sales_when_promoted - avg_sales_without_promo)
            / NULLIF(avg_sales_without_promo, 0)
        ) >= 25
        THEN 'Selective Promo Opportunity'
        ELSE 'Low Promo Sensitivity'
    END AS promo_profile
FROM family_promo
ORDER BY promo_uplift_pct DESC, total_revenue DESC;

-- Insight: Promotion analysis should answer two separate questions: how often
-- a family needs promo support and how strongly it responds when promoted.
-- Recommendation: Reduce broad discounting in high-dependency / low-response
-- families and protect margin by using more targeted promotional design.


-- Q8. How concentrated is revenue across stores versus product families?
WITH store_revenue AS (
    SELECT
        CAST(store_nbr AS CHAR(20)) AS entity_id,
        SUM(sales) AS revenue
    FROM train
    GROUP BY store_nbr
),
store_ranked AS (
    SELECT
        entity_id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn,
        COUNT(*) OVER () AS entity_count,
        SUM(revenue) OVER () AS total_revenue
    FROM store_revenue
),
store_concentration AS (
    SELECT
        'Store Network' AS entity_type,
        MAX(entity_count) AS entity_count,
        ROUND(SUM(POWER(revenue / total_revenue, 2)), 4) AS hhi,
        ROUND(MAX(revenue) / MAX(total_revenue) * 100, 2) AS top_1_share_pct,
        ROUND(
            SUM(CASE WHEN rn <= CEILING(entity_count * 0.20) THEN revenue ELSE 0 END)
            / MAX(total_revenue) * 100,
            2
        ) AS top_20pct_share_pct
    FROM store_ranked
),
family_revenue AS (
    SELECT
        family AS entity_id,
        SUM(sales) AS revenue
    FROM train
    GROUP BY family
),
family_ranked AS (
    SELECT
        entity_id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn,
        COUNT(*) OVER () AS entity_count,
        SUM(revenue) OVER () AS total_revenue
    FROM family_revenue
),
family_concentration AS (
    SELECT
        'Product Family Mix' AS entity_type,
        MAX(entity_count) AS entity_count,
        ROUND(SUM(POWER(revenue / total_revenue, 2)), 4) AS hhi,
        ROUND(MAX(revenue) / MAX(total_revenue) * 100, 2) AS top_1_share_pct,
        ROUND(
            SUM(CASE WHEN rn <= CEILING(entity_count * 0.20) THEN revenue ELSE 0 END)
            / MAX(total_revenue) * 100,
            2
        ) AS top_20pct_share_pct
    FROM family_ranked
)
SELECT *
FROM store_concentration
UNION ALL
SELECT *
FROM family_concentration;

-- Insight: Concentration risk is a portfolio question, not just a store question;
-- high concentration in either stores or categories creates dependency risk.
-- Recommendation: Use HHI and top-share metrics to balance growth bets with
-- resilience planning across the network and the product mix.


-- Q9. Which families over-index by store cluster versus the network average?
WITH cluster_family AS (
    SELECT
        st.cluster,
        t.family,
        SUM(t.sales) AS revenue
    FROM train t
    JOIN stores st
        ON t.store_nbr = st.store_nbr
    GROUP BY st.cluster, t.family
),
cluster_share AS (
    SELECT
        cluster,
        family,
        100.0 * revenue / SUM(revenue) OVER (PARTITION BY cluster) AS cluster_share_pct
    FROM cluster_family
),
network_share AS (
    SELECT
        family,
        100.0 * total_revenue / SUM(total_revenue) OVER () AS network_share_pct
    FROM (
        SELECT
            family,
            SUM(sales) AS total_revenue
        FROM train
        GROUP BY family
    ) x
)
SELECT
    cs.cluster,
    cs.family,
    ROUND(cs.cluster_share_pct, 2) AS cluster_share_pct,
    ROUND(ns.network_share_pct, 2) AS network_share_pct,
    ROUND(cs.cluster_share_pct / NULLIF(ns.network_share_pct, 0), 2) AS sales_index,
    CASE
        WHEN cs.cluster_share_pct / NULLIF(ns.network_share_pct, 0) >= 1.20 THEN 'Over-Indexed'
        WHEN cs.cluster_share_pct / NULLIF(ns.network_share_pct, 0) <= 0.80 THEN 'Under-Indexed'
        ELSE 'In-Line'
    END AS cluster_positioning
FROM cluster_share cs
JOIN network_share ns
    ON cs.family = ns.family
ORDER BY cs.cluster, sales_index DESC, cs.family;

-- Insight: Cluster analysis turns raw sales into localized demand intelligence,
-- revealing where assortments should differ from the company-wide mix.
-- Recommendation: Use over-index and under-index signals to tailor category
-- space, inventory depth, and promotions by cluster instead of one network template.

/* =========================================================
SECTION 4: STORE PRODUCTIVITY, SEGMENTATION & RISK
========================================================= */

-- Q10. What does the store productivity and scale scorecard look like?
WITH store_scorecard AS (
    SELECT
        store_nbr,
        city,
        state,
        store_type,
        cluster,
        SUM(total_sales) AS total_revenue,
        AVG(total_sales) AS avg_store_day_revenue,
        SUM(transactions) AS total_transactions,
        SUM(total_sales) / NULLIF(SUM(transactions), 0) AS sales_per_transaction,
        AVG(promo_family_mix_pct) AS avg_promo_family_mix_pct,
        100.0 * SUM(CASE WHEN is_special_day = 1 THEN total_sales ELSE 0 END) / NULLIF(SUM(total_sales), 0) AS special_day_sales_share_pct
    FROM vw_store_day_context
    GROUP BY store_nbr, city, state, store_type, cluster
)
SELECT
    store_nbr,
    city,
    state,
    store_type,
    cluster,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_store_day_revenue, 2) AS avg_store_day_revenue,
    total_transactions,
    ROUND(sales_per_transaction, 2) AS sales_per_transaction,
    ROUND(avg_promo_family_mix_pct, 2) AS avg_promo_family_mix_pct,
    ROUND(special_day_sales_share_pct, 2) AS special_day_sales_share_pct,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY sales_per_transaction DESC) AS basket_rank
FROM store_scorecard
ORDER BY revenue_rank, basket_rank, store_nbr;

-- Insight: Industry-grade store review should combine scale, basket quality,
-- promo intensity, and event exposure to separate true leaders from fragile leaders.
-- Recommendation: Promote stores only after they are strong on both revenue
-- and basket quality, not on revenue size alone.


-- Q11. Which stores are operationally at risk based on the last 90 days?
WITH max_date AS (
    SELECT MAX(date) AS max_date
    FROM vw_store_day_context
),
store_window AS (
    SELECT
        c.store_nbr,
        MAX(c.city) AS city,
        MAX(c.state) AS state,
        SUM(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 89 DAY) AND m.max_date
                THEN c.total_sales
                ELSE 0
            END
        ) AS recent_90d_revenue,
        SUM(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 179 DAY) AND DATE_SUB(m.max_date, INTERVAL 90 DAY)
                THEN c.total_sales
                ELSE 0
            END
        ) AS prior_90d_revenue,
        SUM(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 89 DAY) AND m.max_date
                THEN c.transactions
                ELSE 0
            END
        ) AS recent_90d_transactions,
        SUM(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 179 DAY) AND DATE_SUB(m.max_date, INTERVAL 90 DAY)
                THEN c.transactions
                ELSE 0
            END
        ) AS prior_90d_transactions,
        AVG(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 89 DAY) AND m.max_date
                THEN c.sales_per_transaction
            END
        ) AS recent_90d_sales_per_transaction,
        AVG(
            CASE
                WHEN c.date BETWEEN DATE_SUB(m.max_date, INTERVAL 179 DAY) AND DATE_SUB(m.max_date, INTERVAL 90 DAY)
                THEN c.sales_per_transaction
            END
        ) AS prior_90d_sales_per_transaction
    FROM vw_store_day_context c
    CROSS JOIN max_date m
    GROUP BY c.store_nbr, m.max_date
),
store_growth AS (
    SELECT
        store_nbr,
        city,
        state,
        recent_90d_revenue,
        prior_90d_revenue,
        ROUND(100.0 * (recent_90d_revenue - prior_90d_revenue) / NULLIF(prior_90d_revenue, 0), 2) AS revenue_growth_pct,
        ROUND(100.0 * (recent_90d_transactions - prior_90d_transactions) / NULLIF(prior_90d_transactions, 0), 2) AS transaction_growth_pct,
        ROUND(100.0 * (recent_90d_sales_per_transaction - prior_90d_sales_per_transaction) / NULLIF(prior_90d_sales_per_transaction, 0), 2) AS basket_growth_pct
    FROM store_window
    WHERE prior_90d_revenue > 0
),
network_stats AS (
    SELECT
        AVG(revenue_growth_pct) AS avg_rev_growth,
        STDDEV_POP(revenue_growth_pct) AS sd_rev_growth,
        AVG(transaction_growth_pct) AS avg_txn_growth,
        STDDEV_POP(transaction_growth_pct) AS sd_txn_growth,
        AVG(basket_growth_pct) AS avg_basket_growth,
        STDDEV_POP(basket_growth_pct) AS sd_basket_growth
    FROM store_growth
)
SELECT
    sg.store_nbr,
    sg.city,
    sg.state,
    sg.recent_90d_revenue,
    sg.prior_90d_revenue,
    sg.revenue_growth_pct,
    sg.transaction_growth_pct,
    sg.basket_growth_pct,
    CASE
        WHEN sg.revenue_growth_pct < (ns.avg_rev_growth - ns.sd_rev_growth)
         AND sg.transaction_growth_pct < (ns.avg_txn_growth - ns.sd_txn_growth)
         AND sg.basket_growth_pct < (ns.avg_basket_growth - ns.sd_basket_growth)
        THEN 'High Risk Outlier (All Metrics Down)'
        WHEN sg.revenue_growth_pct < (ns.avg_rev_growth - ns.sd_rev_growth)
         AND sg.transaction_growth_pct < (ns.avg_txn_growth - ns.sd_txn_growth)
        THEN 'Traffic Risk Outlier'
        WHEN sg.revenue_growth_pct < (ns.avg_rev_growth - ns.sd_rev_growth)
         AND sg.basket_growth_pct < (ns.avg_basket_growth - ns.sd_basket_growth)
        THEN 'Basket Risk Outlier'
        WHEN sg.revenue_growth_pct >= ns.avg_rev_growth
         AND sg.transaction_growth_pct >= ns.avg_txn_growth
        THEN 'Healthy Above Average'
        ELSE 'Watchlist'
    END AS risk_tier
FROM store_growth sg
CROSS JOIN network_stats ns
ORDER BY
    CASE 
        WHEN risk_tier LIKE '%High Risk%' THEN 1
        WHEN risk_tier LIKE '%Traffic Risk%' THEN 2
        WHEN risk_tier LIKE '%Basket Risk%' THEN 3
        WHEN risk_tier = 'Watchlist' THEN 4
        ELSE 5
    END,
    sg.revenue_growth_pct;

-- Insight: Using network-level standard deviations to flag store risk separates true operational degradation from network-wide seasonal downturns.


-- Q12. How should stores be segmented into strategic archetypes?
WITH store_scorecard AS (
    SELECT
        store_nbr,
        city,
        state,
        cluster,
        SUM(total_sales) AS total_revenue,
        SUM(total_sales) / NULLIF(SUM(transactions), 0) AS sales_per_transaction
    FROM vw_store_day_context
    GROUP BY store_nbr, city, state, cluster
),
segmented AS (
    SELECT
        store_nbr,
        city,
        state,
        cluster,
        total_revenue,
        sales_per_transaction,
        NTILE(4) OVER (ORDER BY total_revenue DESC) AS revenue_quartile,
        NTILE(4) OVER (ORDER BY sales_per_transaction DESC) AS basket_quartile
    FROM store_scorecard
)
SELECT
    store_nbr,
    city,
    state,
    cluster,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(sales_per_transaction, 2) AS sales_per_transaction,
    revenue_quartile,
    basket_quartile,
    CASE
        WHEN revenue_quartile = 1 AND basket_quartile = 1 THEN 'Scale + Basket Leader'
        WHEN revenue_quartile = 1 AND basket_quartile >= 2 THEN 'Traffic-Led Scale Store'
        WHEN revenue_quartile IN (2, 3) AND basket_quartile = 1 THEN 'Premium Basket Store'
        WHEN revenue_quartile = 4 AND basket_quartile = 4 THEN 'Turnaround Candidate'
        ELSE 'Middle Portfolio Store'
    END AS store_archetype
FROM segmented
ORDER BY revenue_quartile, basket_quartile, total_revenue DESC;

-- Insight: Strategic segmentation is more useful than straight ranking because
-- different stores need different operating playbooks.
-- Recommendation: Build portfolio actions by archetype so turnaround stores,
-- premium basket stores, and scale leaders are not managed the same way.

/* =========================================================
SECTION 5: CALENDAR EFFECTS, EVENTS & MACRO CONTEXT
========================================================= */

-- Q13. What is the true holiday lift once holidays are mapped to the stores they affect?
WITH holiday_scope_performance AS (
    SELECT
        CASE
            WHEN is_holiday = 0 THEN 'Non-Holiday'
            WHEN holiday_scope = 'Mixed' THEN 'Mixed-Scope Holiday'
            ELSE CONCAT(holiday_scope, ' Holiday')
        END AS day_group,
        AVG(total_sales) AS avg_store_day_revenue,
        AVG(transactions) AS avg_transactions,
        AVG(sales_per_transaction) AS avg_sales_per_transaction,
        COUNT(*) AS store_days
    FROM vw_store_day_context
    GROUP BY day_group
),
baseline AS (
    SELECT
        avg_store_day_revenue AS base_revenue,
        avg_transactions AS base_transactions,
        avg_sales_per_transaction AS base_sales_per_transaction
    FROM holiday_scope_performance
    WHERE day_group = 'Non-Holiday'
)
SELECT
    p.day_group,
    p.store_days,
    ROUND(p.avg_store_day_revenue, 2) AS avg_store_day_revenue,
    ROUND(p.avg_transactions, 2) AS avg_transactions,
    ROUND(p.avg_sales_per_transaction, 2) AS avg_sales_per_transaction,
    ROUND(p.avg_store_day_revenue - b.base_revenue, 2) AS revenue_delta_vs_non_holiday,
    ROUND(100.0 * (p.avg_store_day_revenue - b.base_revenue) / NULLIF(b.base_revenue, 0), 2) AS revenue_lift_pct,
    ROUND(100.0 * (p.avg_transactions - b.base_transactions) / NULLIF(b.base_transactions, 0), 2) AS transaction_lift_pct,
    ROUND(
        100.0 * (p.avg_sales_per_transaction - b.base_sales_per_transaction)
        / NULLIF(b.base_sales_per_transaction, 0),
        2
    ) AS basket_lift_pct
FROM holiday_scope_performance p
CROSS JOIN baseline b
ORDER BY
    CASE p.day_group
        WHEN 'Non-Holiday' THEN 1
        WHEN 'National Holiday' THEN 2
        WHEN 'Regional Holiday' THEN 3
        WHEN 'Local Holiday' THEN 4
        ELSE 5
    END;

-- Insight: Holiday analysis is only credible when calendar events are mapped
-- to the stores they actually affect, especially for local and regional holidays.
-- Recommendation: Use scope-aware holiday lift for campaign planning rather
-- than applying one blanket holiday assumption across the network.


-- Q14. Which event descriptions create the strongest store-day lift?
WITH event_performance AS (
    SELECT
        hm.holiday_scope,
        hm.event_type,
        hm.description,
        COUNT(*) AS affected_store_days,
        AVG(c.total_sales) AS avg_store_day_revenue,
        AVG(c.transactions) AS avg_transactions,
        AVG(c.sales_per_transaction) AS avg_sales_per_transaction
    FROM vw_store_holiday_map hm
    JOIN vw_store_day_context c
        ON hm.date = c.date
       AND hm.store_nbr = c.store_nbr
    GROUP BY hm.holiday_scope, hm.event_type, hm.description
),
baseline AS (
    SELECT
        AVG(total_sales) AS base_revenue,
        AVG(transactions) AS base_transactions,
        AVG(sales_per_transaction) AS base_sales_per_transaction
    FROM vw_store_day_context
    WHERE is_special_day = 0
)
SELECT
    ep.holiday_scope,
    ep.event_type,
    ep.description,
    ep.affected_store_days,
    ROUND(ep.avg_store_day_revenue, 2) AS avg_store_day_revenue,
    ROUND(100.0 * (ep.avg_store_day_revenue - b.base_revenue) / NULLIF(b.base_revenue, 0), 2) AS revenue_lift_pct,
    ROUND(100.0 * (ep.avg_transactions - b.base_transactions) / NULLIF(b.base_transactions, 0), 2) AS transaction_lift_pct,
    ROUND(
        100.0 * (ep.avg_sales_per_transaction - b.base_sales_per_transaction)
        / NULLIF(b.base_sales_per_transaction, 0),
        2
    ) AS basket_lift_pct
FROM event_performance ep
CROSS JOIN baseline b
WHERE ep.affected_store_days >= 20
ORDER BY revenue_lift_pct DESC, ep.affected_store_days DESC
LIMIT 25;

-- Insight: Event descriptions are the closest thing to a merchandising playbook
-- in the calendar table because they show which specific occasions move demand.
-- Recommendation: Promote the occasions with proven lift and rethink events
-- that create complexity without enough revenue or basket benefit.


-- Q15. How sensitive is revenue to different oil-price regimes?
WITH monthly_metrics AS (
    SELECT
        month_start,
        SUM(total_sales) AS revenue,
        SUM(transactions) AS transactions,
        AVG(oil_price) AS avg_oil_price
    FROM vw_store_day_context
    GROUP BY month_start
),
filtered AS (
    SELECT *
    FROM monthly_metrics
    WHERE avg_oil_price IS NOT NULL
),
regimes AS (
    SELECT
        month_start,
        revenue,
        transactions,
        avg_oil_price,
        NTILE(4) OVER (ORDER BY avg_oil_price) AS oil_price_quartile
    FROM filtered
),
correlation_calc AS (
    SELECT
        COUNT(*) AS months_compared,
        (
            COUNT(*) * SUM(avg_oil_price * revenue) - SUM(avg_oil_price) * SUM(revenue)
        ) / NULLIF(
            SQRT(
                (COUNT(*) * SUM(avg_oil_price * avg_oil_price) - POWER(SUM(avg_oil_price), 2)) *
                (COUNT(*) * SUM(revenue * revenue) - POWER(SUM(revenue), 2))
            ),
            0
        ) AS revenue_oil_correlation
    FROM filtered
)
SELECT
    r.oil_price_quartile,
    ROUND(MIN(r.avg_oil_price), 2) AS min_avg_oil_price,
    ROUND(MAX(r.avg_oil_price), 2) AS max_avg_oil_price,
    ROUND(AVG(r.revenue), 2) AS avg_monthly_revenue,
    ROUND(AVG(r.revenue / NULLIF(r.transactions, 0)), 2) AS avg_basket_value,
    c.months_compared,
    ROUND(c.revenue_oil_correlation, 4) AS revenue_oil_correlation
FROM regimes r
CROSS JOIN correlation_calc c
GROUP BY r.oil_price_quartile, c.months_compared, c.revenue_oil_correlation
ORDER BY r.oil_price_quartile;

-- Insight: Macro sensitivity matters when revenue, transactions, or basket
-- change systematically across external-price regimes.
-- Recommendation: Use oil regime analysis as a planning stress test so demand
-- scenarios are not built on internal sales history alone.


-- Q16. Which dates are genuine network-level anomalies, and what explains them?
WITH daily_network AS (
    SELECT
        c.date,
        SUM(c.total_sales) AS network_revenue,
        SUM(c.transactions) AS network_transactions,
        AVG(c.promo_family_mix_pct) AS avg_promo_family_mix_pct,
        MAX(c.is_special_day) AS has_special_day
    FROM vw_store_day_context c
    GROUP BY c.date
),
daily_event_context AS (
    SELECT
        date,
        GROUP_CONCAT(DISTINCT description ORDER BY description SEPARATOR ' | ') AS event_context
    FROM vw_store_holiday_map
    GROUP BY date
),
stats AS (
    SELECT
        AVG(network_revenue) AS avg_revenue,
        STDDEV_POP(network_revenue) AS sd_revenue
    FROM daily_network
),
scored AS (
    SELECT
        d.date,
        d.network_revenue,
        d.network_transactions,
        d.avg_promo_family_mix_pct,
        d.has_special_day,
        ec.event_context,
        (d.network_revenue - s.avg_revenue) / NULLIF(s.sd_revenue, 0) AS z_score
    FROM daily_network d
    CROSS JOIN stats s
    LEFT JOIN daily_event_context ec
        ON d.date = ec.date
)
SELECT
    date,
    ROUND(network_revenue, 2) AS network_revenue,
    network_transactions,
    ROUND(network_revenue / NULLIF(network_transactions, 0), 2) AS basket_value,
    ROUND(avg_promo_family_mix_pct, 2) AS avg_promo_family_mix_pct,
    ROUND(z_score, 2) AS z_score,
    CASE
        WHEN z_score >= 2 THEN 'High Revenue Outlier'
        ELSE 'Low Revenue Outlier'
    END AS outlier_type,
    has_special_day,
    COALESCE(event_context, 'No mapped event context') AS event_context
FROM scored
WHERE ABS(z_score) >= 2
ORDER BY ABS(z_score) DESC, date;

-- Insight: Outlier dates become decision-useful when paired with event and
-- promotion context; otherwise they are just statistical curiosities.
-- Recommendation: Exclude unexplained outliers from baseline forecasting and
-- preserve explained outliers as event playbook candidates.


/* =========================================================
EXECUTIVE FINDINGS FROM SOURCE DATA

- Data covers 2013-01-01 to 2017-08-15 across 54 stores and 33 families.
- The dataset represents roughly 1.074 billion in recorded sales.
- The top 5 product families contribute about 78.72% of total revenue.
- The top 10 stores contribute about 40.17% of total revenue.
- Comparable-store sales are up about 10.0% in 2017 versus 2016 through August 15.
- Holiday-attributed store-days run about 20.58% above non-holiday store-days.
- Monthly revenue shows a strong negative correlation of about -0.7495 with oil prices.
- Sunday is the strongest trading day in the source data, while Thursday is the weakest.

WHAT MAKES THIS FILE INDUSTRY-READY

- Reusable temporary analytical tables reduce repeated heavy scans.
- Holiday logic is mapped to applicable stores by locale, state, and city.
- Comparable-store growth avoids naive topline comparisons.
- Promotion analysis distinguishes dependency from real uplift.
- Store diagnostics separate traffic risk from basket-quality risk.
- Macro sensitivity and anomaly context are integrated into the same analysis layer.

========================================================= */

-- ============================================================
-- FINAL TAKEAWAY
-- ============================================================
-- This project is designed as a MySQL 8 decision-support analysis rather than
-- a basic SQL exercise. It combines executive KPI design, comparable-store logic,
-- portfolio management, event attribution, and operational risk diagnostics into
-- a workflow that is much closer to how retail analytics is expected to operate
-- in industry settings.
