CREATE ACCESS METHOD pgmumbo 
    TYPE INDEX
    HANDLER pgmumbo_amhandler;

CREATE OPERATOR CLASS ops_text
    DEFAULT FOR TYPE text
    USING pgmumbo AS
	OPERATOR	1	=(text, text);
