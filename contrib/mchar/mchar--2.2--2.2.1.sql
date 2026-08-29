CREATE OR REPLACE FUNCTION mvarchar_support(internal)
	RETURNS internal
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE RETURNS NULL ON NULL INPUT
	PARALLEL SAFE;
	
ALTER FUNCTION mvarchar(mvarchar, integer, boolean)
	SUPPORT mvarchar_support;


