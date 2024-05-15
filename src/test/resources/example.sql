SELECT
    CASE WHEN payload.op LIKE 'c' OR payload.op LIKE 'u' OR payload.op LIKE 'r' THEN payload.after
    CASE WHEN payload.op LIKE 'd' THEN payload.before
           END                      as record,
    payload.op                     as op,
    metadata_updated_at            as metadata_updated_at
from bronze_table