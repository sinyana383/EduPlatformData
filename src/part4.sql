-- 111111111 --

create or replace procedure drop_tables(TableName varchar)
as $$
declare
    row record;
    sql text;
begin
    for row in
        select * from information_schema.tables
        where table_name ~ ('^' || TableName)
    loop
    sql = 'drop table if exists ' || row.table_name || ' cascade;';
    execute sql;
    end loop;
end
$$ language plpgsql;

call drop_tables('checks');

-- 333333333 --

create or replace procedure drop_triggers(out res int)
as $$
declare
    row record;
    sql varchar;
begin
    res = 0;
    for row in
        select * from information_schema.triggers
        where event_manipulation in ('INSERT', 'UPDATE', 'DELETE')
    loop
--         raise notice '%', row.trigger_name;
        res = res + 1;
        sql = 'drop trigger if exists ' || row.trigger_name || ' on ' || row.event_object_table || ' cascade;';
    execute sql;
    end loop;
end
$$ language plpgsql;

do
$$
declare
    res int;
begin
    call drop_triggers(res);
    raise notice '%', res;
end
$$;