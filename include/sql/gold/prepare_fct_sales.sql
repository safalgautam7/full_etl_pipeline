-- One row per accepted TMP transaction line. No current-only dimension lookups.
-- Session-local work table: must execute in the same connection as 10_merge.
-- Detect missing OR ambiguous as-of dimension matches; the Airflow task
-- validates MATCH_COUNT fields and business-key uniqueness before merging.
CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.TMP.WORK_FCT_SALES AS
WITH item_matches AS (
    SELECT s.SOURCE_ROW_ID,
           COUNT(i.ITEM_SK) AS ITEM_MATCH_COUNT,
           MAX(i.ITEM_SK) AS ITEM_SK
    FROM RETAIL_DWH.TMP.POS_SALES s
    LEFT JOIN RETAIL_DWH.TGT.DIM_ITEM i
      ON i.ITEM_ID = s.ITEM_ID
     AND i.EFFECTIVE_FROM <= s.SALE_DATE
     AND (s.SALE_DATE < i.EFFECTIVE_TO OR i.EFFECTIVE_TO IS NULL)
    GROUP BY s.SOURCE_ROW_ID
), location_matches AS (
    SELECT s.SOURCE_ROW_ID,
           COUNT(l.LOCN_SK) AS LOCN_MATCH_COUNT,
           MAX(l.LOCN_SK) AS LOCN_SK
    FROM RETAIL_DWH.TMP.POS_SALES s
    LEFT JOIN RETAIL_DWH.TGT.DIM_LOCN l
      ON l.STORE_ID = s.STORE_ID
     AND l.EFFECTIVE_FROM <= s.SALE_DATE
     AND (s.SALE_DATE < l.EFFECTIVE_TO OR l.EFFECTIVE_TO IS NULL)
    GROUP BY s.SOURCE_ROW_ID
)
SELECT
    s.TRANSACTION_ID, s.LINE_NUMBER, s.STORE_ID, s.ITEM_ID,
    s.SALE_DATE, i.ITEM_SK, l.LOCN_SK,
    s.QUANTITY, s.UNIT_PRICE, s.DISCOUNT_AMOUNT, s.NET_SALES,
    s.SOURCE_ROW_ID,
    i.ITEM_MATCH_COUNT, l.LOCN_MATCH_COUNT,
    COUNT(*) OVER (
        PARTITION BY s.TRANSACTION_ID, s.LINE_NUMBER
    ) AS BUSINESS_KEY_OCCURRENCES
FROM RETAIL_DWH.TMP.POS_SALES s
LEFT JOIN item_matches i ON s.SOURCE_ROW_ID = i.SOURCE_ROW_ID
LEFT JOIN location_matches l ON s.SOURCE_ROW_ID = l.SOURCE_ROW_ID
