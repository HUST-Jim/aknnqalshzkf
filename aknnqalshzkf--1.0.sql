-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION aknnqalshzkf" to load this file. \quit

CREATE FUNCTION aknnqalsh_index (text, text, text, int, int, real, int) RETURNS int
AS 'MODULE_PATHNAME', 'aknnqalsh_index'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION aknnqalsh_knn (int, int) RETURNS int
AS 'MODULE_PATHNAME', 'aknnqalsh_knn'
LANGUAGE C IMMUTABLE STRICT;