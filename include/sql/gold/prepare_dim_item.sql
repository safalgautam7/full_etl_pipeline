-- Build the full *observed* historical changes from all available cleaned snapshots.
-- A change in SUBCLASS_NAME alone must NOT create a new DIM_ITEM version.
-- One work table is session-local and has no effect on permanent TGT data.
CREATE OR REPLACE TEMPORARY TABLE RETAIL_DWH.TMP.WORK_DIM_ITEM AS
WITH source_snapshots AS (
    SELECT
        m.SOURCE_ROW_ID, m.SNAPSHOT_DATE, m.ITEM_ID, m.ITEM_NAME,
        m.BRAND, m.UNIT_OF_MEASURE, m.LIST_PRICE, m.SUBCLASS_ID,
        m.DEPARTMENT_ID, m.DEPARTMENT_NAME, m.CLASS_ID, m.CLASS_NAME,
        sc.SUBCLASS_SK, m.IS_ACTIVE,
        HASH(m.ITEM_NAME, m.BRAND, m.UNIT_OF_MEASURE, m.LIST_PRICE,
             m.SUBCLASS_ID, m.IS_ACTIVE, m.DEPARTMENT_ID, m.DEPARTMENT_NAME,
             m.CLASS_ID, m.CLASS_NAME) AS RECORD_HASH
    FROM RETAIL_DWH.TMP.ITEM_MASTER m
    INNER JOIN RETAIL_DWH.TGT.DIM_SUBCLASS sc
        ON m.SUBCLASS_ID = sc.SUBCLASS_ID
), ranked AS (
    SELECT s.*,
        LAG(RECORD_HASH) OVER (
            PARTITION BY ITEM_ID ORDER BY SNAPSHOT_DATE
        ) AS PREVIOUS_HASH
    FROM source_snapshots s
), boundaries AS (
    SELECT *
    FROM ranked
    WHERE PREVIOUS_HASH IS NULL OR RECORD_HASH <> PREVIOUS_HASH
)
SELECT
    b.SOURCE_ROW_ID, b.ITEM_ID, b.ITEM_NAME, b.BRAND,
    b.UNIT_OF_MEASURE, b.LIST_PRICE, b.SUBCLASS_ID, b.SUBCLASS_SK,
    b.DEPARTMENT_ID, b.DEPARTMENT_NAME, b.CLASS_ID, b.CLASS_NAME,
    b.IS_ACTIVE, b.SNAPSHOT_DATE AS EFFECTIVE_FROM,
    LEAD(b.SNAPSHOT_DATE) OVER (
        PARTITION BY b.ITEM_ID ORDER BY b.SNAPSHOT_DATE
    ) AS EFFECTIVE_TO,
    LEAD(b.SNAPSHOT_DATE) OVER (
        PARTITION BY b.ITEM_ID ORDER BY b.SNAPSHOT_DATE
    ) IS NULL AS IS_CURRENT,
    b.RECORD_HASH
FROM boundaries b
