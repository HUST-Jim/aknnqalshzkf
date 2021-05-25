-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_qalsh" to load this file. \quit

CREATE FUNCTION l2_distance (real[], real[]) RETURNS real
AS 'MODULE_PATHNAME', 'l2_distance'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_qalsh_index (text, text, text, int, int, real, int, boolean) RETURNS int
AS 'MODULE_PATHNAME', 'pg_qalsh_index'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_qalsh_knn (int, int) RETURNS int
AS 'MODULE_PATHNAME', 'pg_qalsh_knn'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pg_qalsh_insert_data (int, real[]) RETURNS void
AS 'MODULE_PATHNAME', 'pg_qalsh_isert_data'
LANGUAGE C IMMUTABLE STRICT;