\echo Use "CREATE EXTENSION fulleq FROM unpackaged" to load this file. \quit
DROP OPERATOR CLASS IF EXISTS int2vector_fill_ops USING hash;
DROP OPERATOR FAMILY IF EXISTS int2vector_fill_ops USING hash;
DROP FUNCTION IF EXISTS fullhash_int2vector(int2vector);
DROP OPERATOR IF EXISTS == (int2vector, int2vector);
DROP FUNCTION IF EXISTS isfulleq_int2vector(int2vector, int2vector);
-- For bool

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_bool(bool, bool);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_bool(bool);

ALTER EXTENSION fulleq ADD OPERATOR == (bool, bool);

ALTER EXTENSION fulleq ADD OPERATOR CLASS bool_fill_ops USING hash;

-- For bytea

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_bytea(bytea, bytea);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_bytea(bytea);

ALTER EXTENSION fulleq ADD OPERATOR == (bytea, bytea);

ALTER EXTENSION fulleq ADD OPERATOR CLASS bytea_fill_ops USING hash;

-- For char

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_char(char, char);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_char(char);

ALTER EXTENSION fulleq ADD OPERATOR == (char, char);

ALTER EXTENSION fulleq ADD OPERATOR CLASS char_fill_ops USING hash;

-- For name

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_name(name, name);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_name(name);

ALTER EXTENSION fulleq ADD OPERATOR == (name, name);

ALTER EXTENSION fulleq ADD OPERATOR CLASS name_fill_ops USING hash;

-- For int8

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_int8(int8, int8);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_int8(int8);

ALTER EXTENSION fulleq ADD OPERATOR == (int8, int8);

ALTER EXTENSION fulleq ADD OPERATOR CLASS int8_fill_ops USING hash;

-- For int2

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_int2(int2, int2);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_int2(int2);

ALTER EXTENSION fulleq ADD OPERATOR == (int2, int2);

ALTER EXTENSION fulleq ADD OPERATOR CLASS int2_fill_ops USING hash;

-- For int4

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_int4(int4, int4);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_int4(int4);

ALTER EXTENSION fulleq ADD OPERATOR == (int4, int4);

ALTER EXTENSION fulleq ADD OPERATOR CLASS int4_fill_ops USING hash;

-- For text

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_text(text, text);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_text(text);

ALTER EXTENSION fulleq ADD OPERATOR == (text, text);

ALTER EXTENSION fulleq ADD OPERATOR CLASS text_fill_ops USING hash;

-- For oid

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_oid(oid, oid);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_oid(oid);

ALTER EXTENSION fulleq ADD OPERATOR == (oid, oid);

ALTER EXTENSION fulleq ADD OPERATOR CLASS oid_fill_ops USING hash;

-- For xid

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_xid(xid, xid);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_xid(xid);

ALTER EXTENSION fulleq ADD OPERATOR == (xid, xid);

ALTER EXTENSION fulleq ADD OPERATOR CLASS xid_fill_ops USING hash;

-- For cid

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_cid(cid, cid);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_cid(cid);

ALTER EXTENSION fulleq ADD OPERATOR == (cid, cid);

ALTER EXTENSION fulleq ADD OPERATOR CLASS cid_fill_ops USING hash;

-- For oidvector

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_oidvector(oidvector, oidvector);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_oidvector(oidvector);

ALTER EXTENSION fulleq ADD OPERATOR == (oidvector, oidvector);

ALTER EXTENSION fulleq ADD OPERATOR CLASS oidvector_fill_ops USING hash;

-- For float4

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_float4(float4, float4);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_float4(float4);

ALTER EXTENSION fulleq ADD OPERATOR == (float4, float4);

ALTER EXTENSION fulleq ADD OPERATOR CLASS float4_fill_ops USING hash;

-- For float8

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_float8(float8, float8);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_float8(float8);

ALTER EXTENSION fulleq ADD OPERATOR == (float8, float8);

ALTER EXTENSION fulleq ADD OPERATOR CLASS float8_fill_ops USING hash;

-- For macaddr

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_macaddr(macaddr, macaddr);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_macaddr(macaddr);

ALTER EXTENSION fulleq ADD OPERATOR == (macaddr, macaddr);

ALTER EXTENSION fulleq ADD OPERATOR CLASS macaddr_fill_ops USING hash;

-- For inet

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_inet(inet, inet);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_inet(inet);

ALTER EXTENSION fulleq ADD OPERATOR == (inet, inet);

ALTER EXTENSION fulleq ADD OPERATOR CLASS inet_fill_ops USING hash;

-- For cidr

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_cidr(cidr, cidr);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_cidr(cidr);

ALTER EXTENSION fulleq ADD OPERATOR == (cidr, cidr);

ALTER EXTENSION fulleq ADD OPERATOR CLASS cidr_fill_ops USING hash;

-- For varchar

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_varchar(varchar, varchar);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_varchar(varchar);

ALTER EXTENSION fulleq ADD OPERATOR == (varchar, varchar);

ALTER EXTENSION fulleq ADD OPERATOR CLASS varchar_fill_ops USING hash;

-- For date

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_date(date, date);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_date(date);

ALTER EXTENSION fulleq ADD OPERATOR == (date, date);

ALTER EXTENSION fulleq ADD OPERATOR CLASS date_fill_ops USING hash;

-- For time

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_time(time, time);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_time(time);

ALTER EXTENSION fulleq ADD OPERATOR == (time, time);

ALTER EXTENSION fulleq ADD OPERATOR CLASS time_fill_ops USING hash;

-- For timestamp

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_timestamp(timestamp, timestamp);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_timestamp(timestamp);

ALTER EXTENSION fulleq ADD OPERATOR == (timestamp, timestamp);

ALTER EXTENSION fulleq ADD OPERATOR CLASS timestamp_fill_ops USING hash;

-- For timestamptz

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_timestamptz(timestamptz, timestamptz);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_timestamptz(timestamptz);

ALTER EXTENSION fulleq ADD OPERATOR == (timestamptz, timestamptz);

ALTER EXTENSION fulleq ADD OPERATOR CLASS timestamptz_fill_ops USING hash;

-- For interval

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_interval(interval, interval);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_interval(interval);

ALTER EXTENSION fulleq ADD OPERATOR == (interval, interval);

ALTER EXTENSION fulleq ADD OPERATOR CLASS interval_fill_ops USING hash;

-- For timetz

ALTER EXTENSION fulleq ADD FUNCTION isfulleq_timetz(timetz, timetz);

ALTER EXTENSION fulleq ADD FUNCTION fullhash_timetz(timetz);

ALTER EXTENSION fulleq ADD OPERATOR == (timetz, timetz);

ALTER EXTENSION fulleq ADD OPERATOR CLASS timetz_fill_ops USING hash;

