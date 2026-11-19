-- Exact physical identity coverage, not just equal row counts. Before BEGIN.
SELECT
    (SELECT COUNT(*) FROM {stg}),
    (SELECT COUNT(DISTINCT SOURCE_ROW_ID) FROM {stg}),
    COUNT(*), COUNT(DISTINCT w.SOURCE_ROW_ID),
    COUNT_IF(w.REJECT_REASON IS NULL),
    COUNT_IF(w.REJECT_REASON IS NOT NULL),
    COUNT_IF(s.SOURCE_ROW_ID IS NULL)
FROM {work} w
LEFT JOIN {stg} s ON s.SOURCE_ROW_ID = w.SOURCE_ROW_ID
