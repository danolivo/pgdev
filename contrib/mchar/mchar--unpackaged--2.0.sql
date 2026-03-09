\echo Use "CREATE EXTENSION mchar FROM unpackaged" to load this file. \quit

-- I/O functions

ALTER EXTENSION mchar ADD FUNCTION mchartypmod_in(cstring[]);

ALTER EXTENSION mchar ADD FUNCTION mchartypmod_out(int4);

ALTER EXTENSION mchar ADD FUNCTION mchar_in(cstring);

ALTER EXTENSION mchar ADD FUNCTION mchar_out(mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_send(mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_recv(internal);

ALTER EXTENSION mchar ADD TYPE mchar;

ALTER EXTENSION mchar ADD FUNCTION mchar(mchar, integer, boolean);

ALTER EXTENSION mchar ADD CAST (mchar as mchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_in(cstring);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_out(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_send(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_recv(internal);

ALTER EXTENSION mchar ADD TYPE mvarchar;

ALTER EXTENSION mchar ADD FUNCTION mvarchar(mvarchar, integer, boolean);

ALTER EXTENSION mchar ADD CAST (mvarchar as mvarchar);

--Operations and functions

ALTER EXTENSION mchar ADD FUNCTION length(mchar);

ALTER EXTENSION mchar ADD FUNCTION upper(mchar);

ALTER EXTENSION mchar ADD FUNCTION lower(mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_hash(mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_concat(mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR || (mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_like(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_notlike(mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR ~~ (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR !~~ (mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_regexeq(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_regexne(mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR ~ (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR !~ (mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION similar_escape(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION length(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION upper(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION lower(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_hash(mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_concat(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR || (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_like(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION like_escape(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_notlike(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR ~~ (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR !~~ (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_regexeq(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_regexne(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR ~ (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR !~ (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION similar_escape(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION substr (mchar, int4);

ALTER EXTENSION mchar ADD FUNCTION substr (mchar, int4, int4);

ALTER EXTENSION mchar ADD FUNCTION substr (mvarchar, int4);

ALTER EXTENSION mchar ADD FUNCTION substr (mvarchar, int4, int4);

-- Comparing
--    MCHAR

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_cmp(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_eq(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_ne(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_lt(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_le(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_gt(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_icase_ge(mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR < (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR > (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR <= (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR >= (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR = (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR <> (mchar, mchar); 

ALTER EXTENSION mchar ADD FUNCTION mchar_case_cmp(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_eq(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_ne(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_lt(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_le(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_gt(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_case_ge(mchar, mchar);


ALTER EXTENSION mchar ADD OPERATOR &< (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &> (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &<= (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &>= (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &= (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &<> (mchar, mchar);

--MVARCHAR

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_cmp(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_eq(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_ne(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_lt(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_le(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_gt(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_icase_ge(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR < (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR > (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR <= (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR >= (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR = (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR <> (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_cmp(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_eq(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_ne(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_lt(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_le(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_gt(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_case_ge(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &< (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &> (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &<= (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &>= (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &= (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &<> (mvarchar, mvarchar);

--    MCHAR <> MVARCHAR

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_cmp(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_eq(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_ne(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_lt(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_le(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_gt(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_icase_ge(mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR < (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR > (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR <= (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR >= (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR = (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR <> (mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_cmp(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_eq(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_ne(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_lt(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_le(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_gt(mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mc_mv_case_ge(mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &< (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &> (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &<= (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &>= (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &= (mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR &<> (mchar, mvarchar);

--    MVARCHAR <> MCHAR

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_cmp(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_eq(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_ne(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_lt(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_le(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_gt(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_icase_ge(mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR < (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR > (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR <= (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR >= (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR = (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR <> (mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_cmp(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_eq(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_ne(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_lt(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_le(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_gt(mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mv_mc_case_ge(mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &< (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &> (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &<= (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &>= (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &= (mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR &<> (mvarchar, mchar);

-- MCHAR - VARCHAR operations

ALTER EXTENSION mchar ADD FUNCTION mchar_mvarchar_concat(mchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR || (mchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_mchar_concat(mvarchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR || (mvarchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_mchar(mvarchar, integer, boolean);

ALTER EXTENSION mchar ADD CAST (mvarchar as mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_mvarchar(mchar, integer, boolean);

ALTER EXTENSION mchar ADD CAST (mchar as mvarchar);

-- Aggregates

ALTER EXTENSION mchar ADD FUNCTION mchar_larger(mchar, mchar);

ALTER EXTENSION mchar ADD AGGREGATE max (mchar);

ALTER EXTENSION mchar ADD FUNCTION mchar_smaller(mchar, mchar);

ALTER EXTENSION mchar ADD AGGREGATE min (mchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_larger(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD AGGREGATE max (mvarchar);

ALTER EXTENSION mchar ADD FUNCTION mvarchar_smaller(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD AGGREGATE min (mvarchar);

-- B-tree support
ALTER EXTENSION mchar ADD OPERATOR FAMILY icase_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR FAMILY case_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR CLASS mchar_icase_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR CLASS mchar_case_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR CLASS mchar_icase_ops USING hash;

ALTER EXTENSION mchar ADD OPERATOR CLASS mvarchar_icase_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR CLASS mvarchar_case_ops USING btree;

ALTER EXTENSION mchar ADD OPERATOR CLASS mvarchar_icase_ops USING hash;


-- Index support for LIKE

--mchar_pattern_fixed_prefix could be with wrong number of arguments
ALTER EXTENSION mchar ADD FUNCTION mchar_pattern_fixed_prefix;

ALTER EXTENSION mchar ADD FUNCTION mchar_greaterstring(internal);

ALTER EXTENSION mchar ADD FUNCTION isfulleq_mchar(mchar, mchar);

ALTER EXTENSION mchar ADD FUNCTION fullhash_mchar(mchar);

ALTER EXTENSION mchar ADD OPERATOR == (mchar, mchar);

ALTER EXTENSION mchar ADD OPERATOR CLASS mchar_fill_ops USING hash;

ALTER EXTENSION mchar ADD FUNCTION isfulleq_mvarchar(mvarchar, mvarchar);

ALTER EXTENSION mchar ADD FUNCTION fullhash_mvarchar(mvarchar);

ALTER EXTENSION mchar ADD OPERATOR == (mvarchar, mvarchar);

ALTER EXTENSION mchar ADD OPERATOR CLASS mvarchar_fill_ops USING hash;


