-- SQL Queries to be RUN in Snowflake Environment

-- Receipts Summary VIEW
CREATE OR REPLACE VIEW FETCH.PUBLIC.receipts_summary AS
SELECT 
    rc.receipt_id,
    rc.user_id,
    rc.purchase_date,
    rc.total_spent,
    rc.receipt_status,
    SUM(ri.quantity_purchased) AS total_quantity_purchased,  -- Ensuring accurate item count
    SUM(ri.final_price) AS total_item_value  
FROM FETCH.PUBLIC.receipts_cleaned rc
JOIN FETCH.PUBLIC.receipt_items ri ON rc.receipt_id = ri.receipt_id
LEFT JOIN FETCH.PUBLIC.brands_cleaned b ON ri.item_barcode = b.barcode  
GROUP BY rc.receipt_id, rc.user_id, rc.purchase_date, rc.total_spent, rc.receipt_status;



-- 1&2 What are the top 5 brands by receipts scanned for most recent month? & How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
WITH valid_receipts AS (
    -- Filtering out receipts with valid brands and purchase dates
    SELECT 
        rc.receipt_id,
        b.brand_id,
        DATE_TRUNC('month', rc.purchase_date) AS receipt_month,
        b.brand_code
    FROM fetch.public.receipts_cleaned rc
    JOIN fetch.public.receipt_items ri ON rc.receipt_id = ri.receipt_id
    JOIN fetch.public.brands_cleaned b ON ri.item_barcode = b.barcode
    WHERE rc.purchase_date IS NOT NULL
),
latest_month AS (
    -- Getting the most recent month with valid receipt data
    SELECT MAX(receipt_month) AS latest_month FROM valid_receipts
),
previous_month AS (
    -- Getting the previous month relative to the latest available month
    SELECT DATEADD('month', -1, latest_month) AS previous_month FROM latest_month
),
brand_rankings AS (
    -- Ranking brands based on receipt counts for each month
    SELECT 
        receipt_month,
        brand_id,
        brand_code,
        COUNT(DISTINCT receipt_id) AS receipt_count,  -- Renamed for clarity
        RANK() OVER (PARTITION BY receipt_month ORDER BY COUNT(DISTINCT receipt_id) DESC) AS rank_position
    FROM valid_receipts
    GROUP BY receipt_month, brand_id, brand_code
)
-- Final Selection for Top 5 Brands & Rank Comparison
SELECT 
    latest_ranked.brand_code,
    latest_ranked.receipt_count AS latest_receipt_count,  
    latest_ranked.rank_position AS latest_rank,
    previous_ranked.rank_position AS previous_rank
FROM brand_rankings latest_ranked
LEFT JOIN brand_rankings previous_ranked
    ON latest_ranked.brand_code = previous_ranked.brand_code 
    AND previous_ranked.receipt_month = (SELECT previous_month FROM previous_month)
WHERE latest_ranked.receipt_month = (SELECT latest_month FROM latest_month)
ORDER BY latest_rank
LIMIT 5;



--3&4 When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater? When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
CREATE OR REPLACE VIEW fetch.public.accepted_rejected_summary AS 
SELECT 
    receipt_status,
    AVG(total_spent) AS avg_total_spent,  -- Getting the average spend
    SUM(total_quantity_purchased) AS total_quantity_purchased  -- Getting the total items purchased
FROM fetch.public.receipts_summary
WHERE receipt_status IN ('FINISHED', 'REJECTED')  -- Filtering the correct statuses
GROUP BY receipt_status;


-- Get avg spend & total items for Finished/Rejected Receipts & Get only receipt status & total items purchased
SELECT * FROM fetch.public.finished_rejected_summary;


--5&6 Which brand has the most spend among users who were created within the past 6 months? Which brand has the most transactions among users who were created within the past 6 months?
WITH new_users AS (
    SELECT user_id 
    FROM fetch.public.users_cleaned
    -- The database does not contain receipts from the past 6 months, so instead of using CURRENT_DATE, 
    -- we consider MAX(purchase_date) from the receipts dataset as a reference point.
    WHERE created_date >= DATEADD(MONTH, -6, (SELECT MAX(purchase_date) FROM fetch.public.receipts_cleaned))
),
user_brand_summary AS (
    SELECT 
        b.brand_code,
        SUM(rc.total_spent) AS total_spend,
        COUNT(DISTINCT rc.receipt_id) AS transaction_count
    FROM fetch.public.receipt_items ri
    JOIN fetch.public.receipts_cleaned rc ON ri.receipt_id = rc.receipt_id
    JOIN fetch.public.brands_cleaned b ON ri.item_barcode = b.barcode
    WHERE rc.user_id IN (SELECT user_id FROM new_users)
    GROUP BY b.brand_code
)
SELECT 
    -- Identifying the brand with the highest total spend among new users
    (SELECT brand_code FROM user_brand_summary ORDER BY total_spend DESC LIMIT 1) AS top_brand_by_spend, 
    -- Identifying the brand with the most transactions among new users
    (SELECT brand_code FROM user_brand_summary ORDER BY transaction_count DESC LIMIT 1) AS top_brand_by_transactions;


