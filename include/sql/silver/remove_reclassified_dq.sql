-- Only rejected -> accepted transitions; never touch another source's decision.
DELETE FROM {quarantine}
WHERE SOURCE_TYPE = %s AND SOURCE_ROW_ID IN (
    SELECT SOURCE_ROW_ID FROM {work} WHERE REJECT_REASON IS NULL
)
