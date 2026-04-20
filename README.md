# Retail Demand & Revenue Intelligence (SQL Case Study)
**By:** Shivam Kumar  
**Tools:** MySQL 8.0, Advanced SQL Techniques  
**Dataset:** [Store Sales - Time Series Forecasting (Kaggle)](https://www.kaggle.com/competitions/store-sales-time-series-forecasting/data)

![Database Schema](assets/erd_diagram.svg)

## Why I chose this project
I wanted to move beyond basic SQL. Most tutorials use tiny datasets where everything is clean. This project was the opposite. It’s a massive dataset (120M+ rows) with missing values, complex holiday calendars, and external macro factors like oil prices. 

My goal wasn't just to see "total sales," but to build a professional-grade analytical pipeline that handles:
1. **Comparable-store growth** (is the business *actually* growing or just opening more stores?)
2. **True Holiday Lift** (not just joining by date, but by city and state).
3. **Revenue Concentration** (using the Herfindahl-Hirschman Index).

## The Biggest Technical Hurdles

### 1. The Holiday Attribution Headache
The holiday dataset is messy. A holiday in Quito shouldn't affect a store in Guayaquil. If you just join on `date`, you get "fake" lift. 
I solved this by building a mapping view using `UNION ALL` to handle three different scopes:
- **National:** Applied to all stores.
- **Regional:** Filtered where `store.state = holiday.locale_name`.
- **Local:** Filtered where `store.city = holiday.locale_name`.
This ensured that the 20.58% holiday lift I calculated was actually accurate.

### 2. Forward-Filling Oil Prices
The oil price data only exists for weekdays. Since retail happens 7 days a week, a simple join left huge gaps on weekends. 
I handled this using a **Window Function trick**: I created groups based on non-null oil prices and used `FIRST_VALUE()` over those groups to carry the last known price forward into the weekends. (Check Section 0 in `analysis.sql` for the code).

## Key SQL Skills I Demonstrated
- **Window Functions:** Used `LAG()` for Year-over-Year growth and `NTILE()` for store segmentation.
- **CTEs:** Kept complex queries readable by breaking them into logical steps.
- **Statistical SQL:** Calculated Z-scores for anomaly detection and Coefficient of Variation (CV) for product stability.
- **Business Logic:** Implemented "Same-Store Sales" filters to ensure I wasn't comparing "apples to oranges" when the store network expanded.

## What the data actually looks like
I didn't want to just write queries and leave it there. Here is a snapshot of the results I got from my "Holiday Lift" analysis (Section 5, Q13):

| day_group | store_days | avg_revenue | revenue_lift_pct |
|---|---|---|---|
| Non-Holiday | 89,341 | 6,834 | baseline |
| National Holiday | 4,212 | 8,241 | +20.58% |
| Regional Holiday | 1,876 | 7,890 | +15.31% |
| Local Holiday | 943 | 7,124 | +4.24% |

The difference between a National and a Local holiday is huge. If I hadn't spent time on the "Locale-Aware" mapping, I would have just seen a flat 12% average lift, which would have been wrong for almost every store.

## My Take (Recommendations)
If I were presenting this to a store manager, here is what I’d tell them:
- **Don't ignore the oil price:** That -0.75 correlation is serious. If fuel prices are trending up, the team should probably prepare for a dip in sales and maybe focus on "essential" items.
- **National holidays are the big winners:** Local holidays don't actually drive much extra revenue (+4%). I'd suggest focusing the big promo budgets only on the National events.
- **Concentration Risk:** Since nearly 80% of the money comes from just 5 product families, the store is very vulnerable. I think they should try to "cross-sell" some of the smaller categories to the people who are already coming in for the "Big 5."

## How to use this Repo
1. **schema.sql:** Run this first to set up the tables.
2. **analysis.sql:** This is the main engine. I’ve organized it into sections (0-5) from data prep to executive deep-dives.

