-- Count, uniqueness, classification, and exclusivity checks INSIDE the transaction.
WITH current_dq AS (
    SELECT q.* FROM {quarantine} q
    JOIN {work} w ON w.SOURCE_ROW_ID = q.SOURCE_ROW_ID
    WHERE q.SOURCE_TYPE = %s
)
SELECT
    (SELECT COUNT(*) FROM {tmp} t JOIN {work} w ON t.SOURCE_ROW_ID = w.SOURCE_ROW_ID),
    (SELECT COUNT(DISTINCT t.SOURCE_ROW_ID) FROM {tmp} t JOIN {work} w ON t.SOURCE_ROW_ID = w.SOURCE_ROW_ID),
    (SELECT COUNT(*) FROM current_dq),
    (SELECT COUNT(DISTINCT SOURCE_ROW_ID) FROM current_dq),
    (SELECT COUNT(*) FROM {tmp} t JOIN {work} w ON t.SOURCE_ROW_ID = w.SOURCE_ROW_ID
     WHERE w.REJECT_REASON IS NOT NULL),
    (SELECT COUNT(*) FROM current_dq q JOIN {work} w ON q.SOURCE_ROW_ID = w.SOURCE_ROW_ID
     WHERE w.REJECT_REASON IS NULL),
    (SELECT COUNT(*) FROM {tmp} t
     JOIN {quarantine} q ON q.SOURCE_ROW_ID = t.SOURCE_ROW_ID AND q.SOURCE_TYPE = %s
     JOIN {work} w ON w.SOURCE_ROW_ID = t.SOURCE_ROW_ID)
