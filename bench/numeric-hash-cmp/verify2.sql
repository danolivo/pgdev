SELECT 'groups g_skew' k, count(DISTINCT g_skew)::text v FROM bench
UNION ALL SELECT 'skew: % of rows in top-200 groups',
  round(100.0*(SELECT sum(c) FROM (SELECT count(*) c FROM bench GROUP BY g_skew ORDER BY 1 DESC LIMIT 200) t)/5000000,1)::text
UNION ALL SELECT 'g_mix distinct values', count(DISTINCT g_mix)::text FROM bench_mix
UNION ALL SELECT 'g_mix distinct byte images', count(*)::text FROM (SELECT DISTINCT g_mix::text, pg_column_size(g_mix) FROM bench_mix) t
UNION ALL SELECT 'g_mix pairs equal-value/equal-length/diff-bytes',
  (SELECT count(*)::text FROM (SELECT DISTINCT g_mix, pg_column_size(g_mix) s FROM bench_mix) t
    WHERE (g_mix, s) IN (SELECT g_mix, pg_column_size(g_mix) FROM bench_mix))
UNION ALL SELECT 'toast: storage of g_big', (SELECT string_agg(DISTINCT
     CASE WHEN pg_column_size(g_big) < 100 THEN 'compressed-or-external' ELSE 'inline' END, ',') FROM bench_toast)
UNION ALL SELECT 'toast rows/groups', (SELECT count(*)::text FROM bench_toast) || ' / ' || (SELECT count(DISTINCT g_big)::text FROM bench_toast)
UNION ALL SELECT 'md5 bench', md5(string_agg(t::text, '' ORDER BY i)) FROM (SELECT * FROM bench WHERE i <= 200000) t
UNION ALL SELECT 'md5 mix', md5(string_agg(g_mix::text || pg_column_size(g_mix), '' ORDER BY i)) FROM bench_mix WHERE i <= 200000;
