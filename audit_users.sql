create schema if not exists public; 

create table public.users(
userid serial primary key,
username text not null,
first_name text not null,
last_name text not null,
city varchar(30) not null,
state varchar(30) not null,
email text not null,
phone varchar(30),
is_like_sport boolean default false
);

create schema if not exists audit;

create table audit.users_audit(
audit_ts timestamptz not null default now(),
operation varchar(10)not null,
username text not null default "current_user"(),
before jsonb, 
after  jsonb
);  


CREATE OR REPLACE FUNCTION public.users_audit_trig()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin

                IF      TG_OP = 'INSERT'

                THEN

                        INSERT INTO audit.users_audit (operation, after)

                                VALUES (TG_OP, to_jsonb(NEW));

                        RETURN NEW;

                ELSIF   TG_OP = 'UPDATE'

                THEN

                    IF NEW != OLD THEN
                             INSERT INTO audit.users_audit (operation, before, after)

                                VALUES (TG_OP, to_jsonb(OLD), to_jsonb(NEW));

                    END IF;
                    RETURN NEW;

                ELSIF   TG_OP = 'DELETE'

                THEN

                        INSERT INTO audit.users_audit (operation, before)

                                VALUES (TG_OP, to_jsonb(OLD));

                        RETURN OLD;

                END IF;
end;
$function$ ;

CREATE TRIGGER users_audit_trig
  BEFORE INSERT OR UPDATE OR DELETE
  ON public.users
  FOR EACH ROW
  EXECUTE PROCEDURE public.users_audit_trig();

CREATE OR REPLACE FUNCTION audit.jsonb_diff(l JSONB, r JSONB) RETURNS JSONB AS
$json_diff$
    SELECT jsonb_object_agg(a.key, a.value) FROM
        ( SELECT key, value FROM jsonb_each(l) ) a LEFT OUTER JOIN
        ( SELECT key, value FROM jsonb_each(r) ) b ON a.key = b.key
    WHERE a.value != b.value OR b.key IS NULL;
$json_diff$
    LANGUAGE sql;

/* 
app=# select after->>'userid' as userid , audit.jsonb_diff(before,after) as before_change , audit.jsonb_diff(after,before) as after_change from audit.users_audit where operation='UPDATE';*/
