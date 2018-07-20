create schema if not exists audit;

DO $$
 DECLARE 
 tblname text := 'users';
 v_create_table text;
 v_index text;
 v_part text;
 v_table text ;
 v_child text;
 v_parent_name text;
 v_query text;
 v_create_function text;
 v_create_trg text;
 v_drop_trg text;

BEGIN

 FOR v_table IN select tablename from pg_tables where schemaname='public' and (tablename like '%users%' or tablename like '%user_%')
  LOOP
   tblname:= v_table;
   v_create_table:='create table if not exists audit.'||tblname||'_log( 
   audit_ts timestamptz not null default now(),  
   operation varchar(10)not null, 
   username text not null default "current_user"(), 
   before jsonb, 
   after  jsonb 
   ) partition by RANGE (audit_ts);';
  EXECUTE v_create_table;

-- create partitioned until end of the year
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201807 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-07-01'') TO (''2018-08-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201808 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-08-01'') TO (''2018-09-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201809 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-09-01'') TO (''2018-10-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201810 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-10-01'') TO (''2018-11-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201811 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-11-01'') TO (''2018-12-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201812 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2018-12-01'') TO (''2019-01-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201901 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-01-01'') TO (''2019-02-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201902 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-02-01'') TO (''2019-03-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201903 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-03-01'') TO (''2019-04-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201904 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-04-01'') TO (''2019-05-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201905 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-05-01'') TO (''2019-06-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201906 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-06-01'') TO (''2019-07-01'');';
EXECUTE v_part;
v_part:= 'CREATE TABLE if not exists audit.'||tblname||'_log_201907 PARTITION OF audit.'||tblname||'_log FOR VALUES FROM (''2019-07-01'') TO (''2019-08-01'');';
EXECUTE v_part;

-- create index on each partitioned table


v_query := 'SELECT i.inhrelid::regclass::text AS child FROM pg_inherits i WHERE  i.inhparent =''audit.'||tblname||'_log''::regclass;';
-- RAISE NOTICE '%',v_query;

FOR v_child IN EXECUTE v_query
LOOP
IF to_regclass(''||v_child||'_audit_ts_operation_idx') IS NULL THEN
  v_index:='create index on '||v_child||' (audit_ts desc,operation);';
  EXECUTE v_index;
END IF;
IF to_regclass(''||v_child||'_before_idx') IS NULL THEN
  v_index:='create index on '||v_child||' using GIN(before);';
  EXECUTE v_index;
END IF;
IF to_regclass(''||v_child||'_after_idx') IS NULL THEN
  v_index:='create index on '||v_child||' using GIN(after);';
  EXECUTE v_index;
 END IF;
END LOOP;

v_create_function:= 'CREATE OR REPLACE FUNCTION public.'||tblname||'_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
                IF      TG_OP = ''INSERT''
                THEN
                        INSERT INTO audit.'||tblname||'_log (operation, after)

                                VALUES (TG_OP, to_jsonb(NEW));
                        RETURN NEW;
                ELSIF   TG_OP = ''UPDATE''
                THEN
                    IF NEW != OLD THEN
                             INSERT INTO audit.'||tblname||'_log (operation, before, after)
                                VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));
                    END IF;
                    RETURN NEW;
                ELSIF   TG_OP = ''DELETE''
                THEN
                        INSERT INTO audit.'||tblname||'_log (operation, before)

                                VALUES (TG_OP, to_jsonb(OLD));
                        RETURN OLD;
                END IF;
end;
$function$ ;';
EXECUTE v_create_function;

v_drop_trg := 'DROP TRIGGER IF EXISTS '||tblname||'_audit_trig ON public.'||tblname||';';
EXECUTE v_drop_trg;

v_create_trg:='CREATE TRIGGER '||tblname||'_audit_trig
  BEFORE INSERT OR UPDATE OR DELETE
  ON public.'||tblname||'
  FOR EACH ROW
  EXECUTE PROCEDURE public.'||tblname||'_audit_trig();';
EXECUTE v_create_trg;

END LOOP;

END
$$;

CREATE OR REPLACE FUNCTION audit.jsonb_diff(l JSONB, r JSONB) RETURNS JSONB AS
$json_diff$
    SELECT jsonb_object_agg(a.key, a.value) FROM
        ( SELECT key, value FROM jsonb_each(l) ) a LEFT OUTER JOIN
        ( SELECT key, value FROM jsonb_each(r) ) b ON a.key = b.key
    WHERE a.value != b.value OR b.key IS NULL;
$json_diff$
    LANGUAGE sql;
