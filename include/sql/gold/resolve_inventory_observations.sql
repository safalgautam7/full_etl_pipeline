-- Resolve *every* accepted inventory observation to precisely one historical
-- item and location version as of its SNAPSHOT_DATE.
-- Intermediate work holds all 150 observations; validation before compression
-- ensures no unresolved records are hidden by change-boundary detection.
CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.TMP.WORK_INV_OBSERVATIONS AS
WITH item_matches AS (
    SELECT s.SOURCE_ROW_ID,
           COUNT(i.ITEM_SK) AS ITEM_MATCH_COUNT,
           MAX(i.ITEM_SK) AS ITEM_SK
    FROM RETAIL_DWH.TMP.INVENTORY_SNAPSHOT s
    LEFT JOIN RETAIL_DWH.TGT.DIM_ITEM i
      ON i.ITEM_ID = s.ITEM_ID
     AND i.EFFECTIVE_FROM <= s.SNAPSHOT_DATE
     AND (s.SNAPSHOT_DATE < i.EFFECTIVE_TO OR i.EFFECTIVE_TO IS NULL)
    GROUP BY s.SOURCE_ROW_ID
), location_matches AS (
    SELECT s.SOURCE_ROW_ID,
           COUNT(l.LOCN_SK) AS LOCN_MATCH_COUNT,
           MAX(l.LOCN_SK) AS LOCN_SK
    FROM RETAIL_DWH.TMP.INVENTORY_SNAPSHOT s
    LEFT JOIN RETAIL_DWH.TGT.DIM_LOCN l
      ON l.STORE_ID = s.STORE_ID
     AND l.EFFECTIVE_FROM <= s.SNAPSHOT_DATE
     AND (s.SNAPSHOT_DATE < l.EFFECTIVE_TO OR l.EFFECTIVE_TO IS NULL)
    GROUP BY s.SOURCE_ROW_ID
)
SELECT
    s.SOURCE_ROW_ID, s.STORE_ID, s.ITEM_ID, s.SNAPSHOT_DATE,
    s.QUANTITY_ON_HAND, s.UNIT_COST, s.INVENTORY_VALUE,
    i.ITEM_SK, l.LOCN_SK,
    i.ITEM_MATCH_COUNT, l.LOCN_MATCH_COUNT,
    COUNT(*) OVER (
        PARTITION BY s.STORE_ID, s.ITEM_ID, s.SNAPSHOT_DATE
    ) AS DUPLICATE_POSITION_DAY,
    HASH(s.QUANTITY_ON_HAND, s.UNIT_COST, s.INVENTORY_VALUE,
         i.ITEM_SK, l.LOCN_SK) AS POSITION_STATE_HASH
FROM RETAIL_DWH.TMP.INVENTORY_SNAPSHOT s
LEFT JOIN item_matches i ON s.SOURCE_ROW_ID = i.SOURCE_ROW_ID
LEFT JOIN location_matches l ON s.SOURCE_ROW_ID = l.SOURCE_ROW_ID
