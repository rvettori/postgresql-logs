CREATE TABLE logs (
    id bigserial primary key,
    table_name text not null,
    pg_user text,
    txts TIMESTAMP WITH TIME ZONE NOT NULL,
    txid bigint,
    app text,
    addr inet,
    port text,      
    query text,     
    op TEXT NOT NULL CHECK (op IN ('I','D','U', 'T')), 
    data hstore,       
    changed hstore
);


CREATE OR REPLACE FUNCTION log_if_changed() RETURNS TRIGGER AS $body$
DECLARE
    log_row logs;
    excluded_cols text[] = ARRAY[]::text[];
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'log_if_changed)_ may only run as an AFTER trigger';
    END IF;

    log_row = ROW(
      nextval('logs_id_seq'),
      TG_TABLE_NAME::TEXT,
      session_user::text,
      current_timestamp,
      txid_current(),
      current_setting('application_name'),
      inet_client_addr()::text,
      inet_client_port()::text,
      current_query(),
      substring(TG_OP,1,1),
      null,null
    );

    IF TG_ARGV[0] IS NOT NULL THEN
        excluded_cols = TG_ARGV[0]::text[];
    END IF;
    
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        log_row.data = hstore(OLD.*) - excluded_cols;
        log_row.changed =  (hstore(NEW.*) - log_row.data) - excluded_cols;
        IF log_row.changed = hstore('') THEN
            RETURN NULL;
        END IF;
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
        log_row.data = hstore(OLD.*) - excluded_cols;
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
        log_row.data = hstore(NEW.*) - excluded_cols;
    ELSIF (TG_LEVEL = 'STATEMENT' AND TG_OP IN ('INSERT','UPDATE','DELETE','TRUNCATE')) THEN
      -- DO NOTHING
    ELSE
        RAISE EXCEPTION '[log_if_changed] - Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;
    INSERT INTO logs VALUES (log_row.*);
    RETURN NULL;
END;
$body$
language 'plpgsql';



CREATE OR REPLACE FUNCTION log_enabled_for(target_table regclass, ignored_cols text[]) RETURNS void AS $body$
DECLARE
  stm_targets text = 'INSERT OR UPDATE OR DELETE OR TRUNCATE';
  _q_txt text;
  _ignored_cols text = '';
BEGIN
    EXECUTE 'DROP TRIGGER IF EXISTS log_trigger_row ON ' || quote_ident(target_table::TEXT);
    EXECUTE 'DROP TRIGGER IF EXISTS log_trigger_stm ON ' || quote_ident(target_table::TEXT);

    -- LOG EACH ROWS
    IF array_length(ignored_cols,1) > 0 THEN
        _ignored_cols = quote_literal(ignored_cols);
    END IF;
    _q_txt = 'CREATE TRIGGER log_trigger_row AFTER INSERT OR UPDATE OR DELETE ON ' || 
             quote_ident(target_table::TEXT) || 
             ' FOR EACH ROW EXECUTE PROCEDURE log_if_changed(' || _ignored_cols || ');';
    RAISE NOTICE '%',_q_txt;
    EXECUTE _q_txt;

    -- LOG STATEMENT, TRUNCATE ONLY
    stm_targets = 'TRUNCATE';
    _q_txt = 'CREATE TRIGGER log_trigger_stm AFTER ' || stm_targets || ' ON ' ||
             target_table ||
             ' FOR EACH STATEMENT EXECUTE PROCEDURE log_if_changed(' || _ignored_cols || ');';
    RAISE NOTICE '%',_q_txt;
    EXECUTE _q_txt;

END;
$body$
language 'plpgsql';

CREATE OR REPLACE FUNCTION log_enabled_for(target_table regclass) RETURNS void AS $$
SELECT log_enabled_for($1, null);
$$ LANGUAGE 'sql';


CREATE OR REPLACE FUNCTION log_disabled_for(target_table regclass) RETURNS void AS $body$
BEGIN
    EXECUTE 'ALTER TABLE ' || quote_ident(target_table::TEXT) || ' DISABLE TRIGGER log_trigger_row ';
    EXECUTE 'ALTER TABLE ' || quote_ident(target_table::TEXT) || ' DISABLE TRIGGER log_trigger_stm ';
END;
$body$
language 'plpgsql';


/**  -- USING BY EXAMPLE

CREATE TABLE NOTES (
  ID SERIAL PRIMARY KEY,
  NOTE TEXT
);


select log_enabled_for('notes');
-- select log_enabled_for('notes', '{id}'::text[]);
-- SELECT log_disabled_for('notes');


-- RECOVER TABLE FORMAT, EX:
-- TO RECORD
select  txid, txts, (populate_record( null::notes  , data)).*, (populate_record(null::notes, changed)).note from logs where op = 'U'
-- KEY VALUE COLUMNS
select  (each(data)).*  from logs where op = 'U'
-- MERGE HSTORE OLD DATA WITH NEWCHANGED
select data, changed, data || changed  as new from logs

*/