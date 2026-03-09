CREATE FUNCTION similar_to_escape(mchar)
RETURNS mchar
AS 'MODULE_PATHNAME', 'mchar_similar_escape'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION similar_to_escape(mchar, mchar)
RETURNS mchar
AS 'MODULE_PATHNAME', 'mchar_similar_escape'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION similar_to_escape(mvarchar)
RETURNS mvarchar
AS 'MODULE_PATHNAME', 'mvarchar_similar_escape'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION similar_to_escape(mvarchar, mvarchar)
RETURNS mvarchar
AS 'MODULE_PATHNAME', 'mvarchar_similar_escape'
LANGUAGE C IMMUTABLE;

