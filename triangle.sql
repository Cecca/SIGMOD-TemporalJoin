CREATE TABLE R1 AS
SELECT 
    column0 as A,
    column1 as B,
    column2 as ts,
    column3 as te
FROM read_csv_auto('data/deduped.csv');
-- FROM read_csv_auto('data/test-db/R1_AB.txt');

CREATE TABLE R2 AS
SELECT 
    column0 as B,
    column1 as C,
    column2 as ts,
    column3 as te
FROM read_csv_auto('data/deduped.csv');

CREATE TABLE R3 AS
SELECT 
    column0 as A,
    column1 as C,
    column2 as ts,
    column3 as te
FROM read_csv_auto('data/deduped.csv');

.timer on

WITH
    -- First, find all triangles for which the intersection of 
    -- intervals is non empty.
    -- The output might contain overlapping intervals that need coalescing,
    -- which will be handled next
    all_triangles AS (
        SELECT
            R1.A as A,
            R2.B as B,
            R3.C as C,
            greatest(R1.ts, R2.ts, R3.ts) as ts,
            least(R1.te, R2.te, R3.te) as te
        FROM R1, R2, R3
        WHERE R1.B = R2.B
          AND R2.C = R3.C
          AND R1.A = R3.A
          AND greatest(R1.ts, R2.ts, R3.ts) < least(R1.te, R2.te, R3.te)
    ),
    -- To coalesce intervals we adapt the code found in this post:
    -- https://chaoxuprime.com/posts/2019-04-27-union-of-intervals-in-sql.html
    -- The idea is that we simulate a line-sweep algorithm: the first step is to
    -- create a relation of endpoints such that each endpoint is tagged
    -- with the number of active intervals at that point, for any triangle.
    -- Therefore, the number of start of a coalesced interval will be the first
    -- endpoint with a non-zero count, and its end will be the first subsequent
    -- endpoint with a zero count
    endpoints AS (
        SELECT A, B, C, t, sum(oi) as open_intervals
        FROM (
            SELECT A, B, C, ts as t, 1 as oi FROM all_triangles
            UNION ALL
            SELECT A, B, C, te as t, -1 as oi FROM all_triangles
        )
        GROUP BY ALL
    ),
    endpoints_with_coverage AS (
        SELECT A, B, C, t, 
            sum(open_intervals) OVER (PARTITION BY A, B, C ORDER BY t) - open_intervals AS coverage
        FROM endpoints
    ),
    equivalence_classes AS (
        SELECT A, B, C, t, COUNT(CASE WHEN coverage=0 THEN 1 END) OVER (PARTITION BY A, B, C ORDER BY t) AS class
        FROM endpoints_with_coverage
    ),
    triangles AS (
        SELECT A, B, C, min(t) as ts, max(t) as te
        FROM equivalence_classes 
        GROUP BY A, B, C, class
    )
SELECT * -- count(*)
FROM triangles
ORDER BY all;

-- SELECT * FROM (
-- SELECT 
--     R1.A as A, R2.B as B, R3.C as C,
--     MAX(GREATEST(R1.ts, R2.ts, R3.ts)) as ts,
--     MIN(LEAST(R1.te, R2.te, R3.te)) as te
-- FROM R1, R2, R3
-- WHERE R1.B = R2.B
--   AND R2.C = R3.C
--   AND R1.A = R3.A
--   AND GREATEST(R1.ts, R2.ts, R3.ts) < LEAST(R1.te, R2.te, R3.te)
-- GROUP BY R1.A, R2.B, R3.C
-- ORDER BY ALL
-- );
--
--
