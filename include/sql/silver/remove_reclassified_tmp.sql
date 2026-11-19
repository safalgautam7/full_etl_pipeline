-- Only accepted -> rejected transitions. Unchanged accepted rows stay untouched.
DELETE FROM {tmp}
WHERE SOURCE_ROW_ID IN (
    SELECT SOURCE_ROW_ID FROM {work} WHERE REJECT_REASON IS NOT NULL
)
