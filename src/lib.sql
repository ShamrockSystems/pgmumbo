CREATE ACCESS METHOD pgmumbo 
    TYPE INDEX
    HANDLER pgmumbo_amhandler;

CREATE OPERATOR pg_catalog.?= (
    PROCEDURE = anyelement_cmpfunc,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = pgmumbo.query
);

CREATE OPERATOR CLASS pgmumbo_ops_anyelement
    USING pgmumbo AS
    OPERATOR 1 pg_catalog.?=(anyelement, pgmumbo.query);
