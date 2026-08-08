-- A short-header numeric occupies 3 + 2*ndigits bytes, so pg_column_size
-- tells us exactly which lane of the fast path a column takes:
--   ndigits <= 4                  -> int64 lane (the profitable one)
--   ndigits 5..9, width cap ok    -> checked 128-bit op per digit
--   (weight+1)*4 + dscale > 39    -> rejected outright, state promotes
\pset title 'fast-path lane per value column'
SELECT col, ndigits, dscale, lane FROM (
  SELECT 'v_narrow' col, (pg_column_size(v_narrow)-3)/2 ndigits, scale(v_narrow) dscale FROM agg WHERE i = 4321
  UNION ALL SELECT 'v_mixscale', (pg_column_size(v_mixscale)-3)/2, scale(v_mixscale) FROM agg WHERE i = 4321
  UNION ALL SELECT 'v_mixscale(odd)', (pg_column_size(v_mixscale)-3)/2, scale(v_mixscale) FROM agg WHERE i = 4322
  UNION ALL SELECT 'v_slow', (pg_column_size(v_slow)-3)/2, scale(v_slow) FROM wide WHERE i = 4321
  UNION ALL SELECT 'v_huge', (pg_column_size(v_huge)-3)/2, scale(v_huge) FROM wide WHERE i = 4321
  UNION ALL SELECT 'v_ovf',  (pg_column_size(v_ovf)-3)/2,  scale(v_ovf)  FROM wide WHERE i = 4321
) t, LATERAL (SELECT CASE
    WHEN ndigits <= 4 THEN 'int64 lane'
    WHEN (ndigits-1)*4 + dscale > 39 THEN 'rejected by width cap -> promotes'
    ELSE 'checked 128-bit per digit' END lane) l;

\pset title 'dataset shape'
SELECT 'rows agg / wide / par' k, (SELECT count(*) FROM agg)::text || ' / ' ||
       (SELECT count(*) FROM wide)::text || ' / ' || (SELECT count(*) FROM par)::text v
UNION ALL SELECT 'groups 1k / 592k / uniq',
       (SELECT count(DISTINCT g_1k) FROM agg)::text || ' / ' ||
       (SELECT count(DISTINCT g_592k) FROM agg)::text || ' / ' ||
       (SELECT count(DISTINCT g_uniq) FROM agg)::text
UNION ALL SELECT 'skew: % of rows in top-200 of 1000 groups',
  round(100.0*(SELECT sum(c) FROM (SELECT count(*) c FROM agg GROUP BY g_skew ORDER BY 1 DESC LIMIT 200) t)
        / (SELECT count(*) FROM agg), 1)::text
UNION ALL SELECT 'v_ovf: rows before int128 overflow in one group',
  (SELECT count(*)::text FROM (SELECT i, sum(v_ovf) OVER (ORDER BY i) s FROM wide) z WHERE s < 170141183460469231731687303715884105727::numeric)
UNION ALL SELECT 'v_slow: full single-group sum stays in int128',
  (SELECT (sum(v_slow) < 170141183460469231731687303715884105727::numeric)::text FROM wide)
UNION ALL SELECT 'md5 agg(sample)', (SELECT md5(string_agg(t::text,'' ORDER BY i)) FROM (SELECT * FROM agg WHERE i <= 100000) t)
UNION ALL SELECT 'md5 wide(sample)', (SELECT md5(string_agg(t::text,'' ORDER BY i)) FROM (SELECT * FROM wide WHERE i <= 100000) t)
UNION ALL SELECT 'md5 par(sample)', (SELECT md5(string_agg(t::text,'' ORDER BY i)) FROM (SELECT * FROM par WHERE i <= 100000) t)
UNION ALL SELECT 'total size', pg_size_pretty(pg_database_size(current_database()));
