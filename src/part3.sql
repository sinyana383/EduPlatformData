--------------------1111111----------------------------
create or replace function f01()
returns table (peer1 varchar, peer2 varchar, "PointsAmount" int)
as $$
begin
return query 
select
    tp1."CheckingPeer" as "Peer1", tp1."CheckedPeer" as "Peer2",
        coalesce(tp1."PointsAmount",0) - coalesce(tp2."PointsAmount", 0) as "PointAmount"
from transferredpoints as tp1
full join transferredpoints as tp2 on tp2."CheckingPeer" = tp1."CheckedPeer"
            and tp1."CheckingPeer" = tp2."CheckedPeer"
where
    tp1."ID" > tp2."ID"
    or
    tp2."ID" is NULL;
end
$$ language plpgsql;


select * from f01();

-------------------2222222----------------------------

create or replace function f02()
returns table ("Peer" varchar, "Task" varchar, "XP" bigint)
as $$
begin
return query 
with t_check as (
select checks."ID", checks."Peer", checks."Task", p2p."State" as p2p_st, verter."State" as ver_st
from checks
    join p2p on p2p."Check" = checks."ID" and (p2p."State" = 'Success'
        )
    left join verter on verter."Check" = checks."ID"
        and (verter."State" = 'Success' or verter."State" is NULL))
    select t_check."Peer", t_check."Task", xp."XPAmount" from t_check
    join xp on xp."Check"=t_check."ID";
end
$$ language plpgsql
;

select * from f02();
---------------------33333333------------------------

create or replace function f03 (day date) returns table(peer varchar) as
$$
begin
return query
    with t1 as(
    select "Peer", max("Date") as date, max("Time") as time from (select * from timetracking
    where "State" = 1 and "Date" < day or ("Date" = day and "Time" = '00:00:00' ) order by 3,4) as p1
    group by "Peer")
    select aaa."Peer" from (
        select t1."Peer", min(timetracking."Date") as minDate from timetracking
        join t1 on t1."Peer" = timetracking."Peer"
        where timetracking."State" = 2 and timetracking."Date" >= t1.date
        group by t1."Peer"
    ) as aaa
    where aaa.minDate > day;
end
$$
language plpgsql;

select * from f03('2023-03-03');

--------------------444444444------------------------

create or replace procedure p04(res refcursor default 'p04')
as $$
begin
open res for
select Peers."Nickname", 
		(coalesce ((select sum ("PointsAmount") from TransferredPoints where Peers."Nickname"=TransferredPoints."CheckingPeer"),0)
		 - coalesce ((select sum ("PointsAmount") from TransferredPoints where Peers."Nickname"=TransferredPoints."CheckedPeer"),0)) PointsChange
from Peers
order by 2;
end
$$ language plpgsql
;


begin;
call p04();
FETCH ALL FROM "p04";
END;

--------------------555555555-------------------------

create or replace procedure p05(res refcursor default 'p05')
as $$
begin
OPEN res FOR
    with dynamic_peers as(
    select changes.peer2 as "Peer", sum(points) as "PointsChange" from(
        with reverse as(
        select peer2, peer1, ("PointsAmount"*-1) as points from f01())

        select * from reverse
        union
        select * from f01()
    ) as changes
    group by changes.peer2)

    select "Nickname" as "Peer", coalesce("PointsChange", 0) as "PointsChange"  from peers
    left join dynamic_peers d on peers."Nickname" = d."Peer"
    order by 2;
end
$$ language plpgsql;

begin;
call p05();
FETCH ALL FROM "p05";
END;

--------------------666666666-------------------------

create or replace procedure p06(res refcursor default 'p06')
as $$
begin
OPEN res FOR
    with num as(
    select checks."Task", "Date", count(checks."Task") as counts from checks
    group by "Date", checks."Task")

    SELECT "Date", n."Task" FROM num as n
    WHERE counts = (
       SELECT MAX(counts)
       FROM (select * from num where n."Date" = num."Date") as aa
    );
end
$$ language plpgsql;

begin;
call p06();
FETCH ALL FROM "p06";
END;

--------------------777777777-------------------------
create or replace procedure p07(
    in block varchar,
    in res refcursor = 'p07'
) as
    $$
DECLARE
    block_count_max int := (select count("Title") from tasks
                            where "Title" ~ (block || '[0-9]'));
begin
    open res for
        with end_task as (select distinct Checks."Peer", Checks."Task", Checks."Date"
                          from Checks
                                join p2p on p2p."Check"=Checks."ID"
                                left join verter on verter."Check"=Checks."ID"
                          where checks."Task" ~ (block || '[0-9]')
                                and p2p."State" = 'Success'
                                and  (verter."State" is NULL or verter."State" = 'Success')
                          order by 1, 2, 3 desc),
            max_count as (select "Peer", count("Peer") as c_p, max("Date") as Day
                          from  end_task
                          group by 1)
    select "Peer", Day from max_count
    where c_p = block_count_max
    order by 2 desc;
end
$$ LANGUAGE plpgsql;

begin;
call p07('C');
fetch all from "p07";
end;

--------------------888888888-------------------------

create or replace procedure p08(res refcursor default 'p08')
as $$
begin
OPEN res FOR
    with rec_count as(
        select "Nickname",
        case when "RecomendedPeer" = "Nickname" then null
        else "RecomendedPeer" end as "RecomendedPeer", count("RecomendedPeer") as counts
        from(select "Peer1", "Peer2" from friends union select "Peer2", "Peer1" from friends) as aaa
        right join peers on "Peer1" = peers."Nickname"
        left join recommendations r on "Peer2" = r."Peer"
        group by "Nickname", "RecomendedPeer"),
    rec_count_nulls as (select "Nickname", "RecomendedPeer",
                        case when "RecomendedPeer" is null then 0
                        else counts end as counts from rec_count)

    select distinct on ("Nickname")
        "Nickname", coalesce(r."RecomendedPeer", 'no friend or recommendations') as "RecommendedPeer"
    from rec_count_nulls r
    where r.counts = (select max(counts) from
          (select * from rec_count_nulls re where r."Nickname" = re."Nickname") as aaa);
end
$$ language plpgsql;

begin;
call p08();
FETCH ALL FROM "p08";
END;

--------------------999999999-------------------------

create or replace procedure p09(
    in first_block varchar,
    in second_block varchar,
    res refcursor  default 'p09'
) as $$
declare
    count_peer int := (select count(*) from peers);
begin
    open res for
        with table1 as (select Checks."Peer" from Checks
                                where checks."Task" ~ (first_block || '[0-9]')
                                group by Checks."Peer"),
            table2 as (select Checks."Peer" from Checks
                                where checks."Task" ~ (second_block || '[0-9]')
                                group by Checks."Peer"),
            both_block as (select * from table1
                                    intersect
                            select * from table2),
            not_start as (select "Nickname" from peers
                            except
                            (select * from table1
                                      union
                            select * from table2))
    select round(
                       ((select count(*) * 100 from table1) / count_peer) -
                       (select count(*) * 100 from both_block) / count_peer, 0
               ) as "StartedBlock1",
        round (
        ((select count(*) * 100 from table2) / count_peer) -
                        (select count(*) * 100 from both_block) / count_peer, 0
               ) as "StartedBlock2",
        round (
            (select count (*) * 100 from both_block) / count_peer, 0
        ) as "StartedBothBlocks",
        round (
            (select count (*) * 100 from not_start) / count_peer, 0
        ) as "DidntStartAnyBlock";
end;
$$ language plpgsql;

BEGIN;
CALL p09('C','CPP' );
FETCH ALL FROM "p09";
END;
--------------------10 10 10 10 10-------------------------

create or replace procedure p10(res refcursor default 'p10')
as $$
begin
OPEN res FOR
    with birthday_checks as(
        select "Nickname", coalesce (xp."Check", 0) as main_status
        from
            (select * from checks
            join peers p on p."Nickname" = checks."Peer"
            where (select extract(day from "Birthday")) = (select extract(day from "Date"))
            and (select extract(month from "Birthday")) = (select extract(month from "Date"))) as b

        left join xp on xp."Check" = b."ID"
        group by "Nickname", main_status)

    select round((select count(distinct b."Nickname") from birthday_checks b where main_status > 0)::numeric * 100 /
                 count(peers."Nickname")::numeric) as "SuccessfulChecks",
           round((select count(distinct b."Nickname") from birthday_checks b where main_status = 0)::numeric * 100 /
           count(peers."Nickname")::numeric) as "UnsuccessfulChecks"  from peers;
end
$$ language plpgsql;

begin;
call p10();
FETCH ALL FROM "p10";
END;

---------------------------------11 11 11 11-----------------------

create or replace procedure p11(task1 varchar, task2 varchar, task3 varchar, res refcursor default 'p11')
as $$
begin
OPEN res FOR
    with two_suc as(
    select "Peer", count("Peer") from(
        select "Peer", "Task" from
            ((select * from checks
                      join xp x on checks."ID" = x."Check"
                      where "Task" = task1)
            union
            (select * from checks
                      join xp x on checks."ID" = x."Check"
                      where "Task" = task2) ) a1
        group by "Peer", "Task") a2
    group by "Peer"
    having count("Peer") = 2)

    select "Peer" from two_suc
    except
    (select two_suc."Peer" from two_suc
    join checks c on c."Peer" = two_suc."Peer"
    join xp x on c."ID" = x."Check"
    where "Task" = task3);
end
$$ language plpgsql;

BEGIN;
CALL p11('C2_SimpleBashUtils','CPP1_s21_matrix','DO5_SimpleDocker' );
FETCH ALL FROM "p11";
END;

---------------------------------12 12 12 12-----------------------

create or replace procedure p12(res refcursor default 'p12')
as $$
begin
OPEN res FOR
    with recursive go_up as
    (
    select (select "Title" from tasks where "ParentTask" is null) as "Title", 0 as "PrevCount"
    union all
    select tasks."Title", "PrevCount" + 1 from go_up
        join tasks on tasks."ParentTask" = go_up."Title"
    )
    select * from go_up;
end
$$ language plpgsql;

BEGIN;
CALL p12( );
FETCH ALL FROM "p12";
END;

---------------------------------13 13 13 13-----------------------
CREATE OR REPLACE PROCEDURE p13(N int ,res refcursor default 'p13')
LANGUAGE plpgsql
AS $$
declare
    cur_d date = null;
    dates date[] = '{}';
    times int = 0;
    row1 record;
begin
    for row1 in
        with all_ch as(
            select checks."ID","Date","Time","XPAmount","MaxXP"
                , "XPAmount"::numeric/"MaxXP"::numeric as "Percent"
            from checks
            left join p2p p on checks."ID" = p."Check"
            left join xp x on checks."ID" = x."Check"
            left join tasks on checks."Task" = tasks."Title"
            where p."State" = 'Start'
            order by "Date")
        select * from all_ch
    loop
        -- change day
        if (cur_d is null or cur_d != row1."Date") then
            cur_d = row1."Date";
            times = 0;
        end if;
        -- count checks
        if (row1."Percent" is not null and row1."Percent" >= 0.8) then
            times = times + 1;
        else times = 0;
        end if;
        -- add date
        if times >= N then
            dates = array_append(dates, row1."Date");
        end if;
    end loop;
   OPEN res FOR
    select distinct unnest(dates);
end $$;

BEGIN;
CALL p13(3);
FETCH ALL FROM "p13";
END;

---------------------------------14 14 14 14-----------------------
create or replace procedure p14 (
    res refcursor default 'p14'
)
as $$
    begin
    open res for
    select "Peer", sum (maxXp) as XP from (
    select Checks."Peer", max("XPAmount") as maxXP from XP
    left join Checks on Checks."ID"=XP."Check"
    group by Checks."Task", Checks."Peer") as aa
group by 1
order by 2 desc
limit 1;
end;
$$ language plpgsql;

BEGIN;
CALL p14();
FETCH ALL FROM "p14";
END;

---------------------------------15 15 15 15-----------------------
create or replace procedure p15( "time" time, N int,
res refcursor default 'p15')
as $$
begin
OPEN res FOR
    with counts_before as(
    select "Peer", count("State") as counts from timetracking
    where "State" = 1 and "Time" < "time"
    group by "Peer")

    select "Peer" from counts_before where counts > N;
end
$$ language plpgsql;

BEGIN;
CALL p15( '23:58:12'::time, 8);
FETCH ALL FROM "p15";
END;

---------------------------------16 16 16 16-----------------------
create or replace procedure p16( N int, M int,
res refcursor default 'p16')
as $$
begin
OPEN res FOR
with all_out as (
    select * from timetracking
    where "State" = 2 and "Date" >= (now() - (N - 1 || ' days')::interval)::date
    and "Date" <= now()::date)
select "Peer" as k from all_out
group by "Peer"
having  count("State") > M;
end
$$ language plpgsql;

BEGIN;
CALL p16( 6, 2);
FETCH ALL FROM "p16";
END;

---------------------------------17 17 17 17-----------------------

create or replace procedure p17(res refcursor default 'p17')
as $$
begin
OPEN res FOR
with general_in as(
    select  "Date", "Time"
    from timetracking
    join peers p on p."Nickname" = timetracking."Peer"
    where (select extract(month from "Birthday")) = (select extract(month from "Date"))
    and "State" = 1)

    ,all_m as (select generate_series('2000-01-01'::date, '2000-12-01'::date, '1 month') as month)

    ,early_in as (
    select "month", count("Date") as counts from all_m
    left join general_in on extract(month from "Date") = extract(month from month)
    where "Time" < '12:00:00'::time
    group by "month")

    ,all_in as (
    select "month", count("Date") as counts from all_m
    left join general_in on extract(month from "Date") = extract(month from month)
    group by "month"
    order by 1)

select TO_CHAR(all_in.month, 'Month') as "Month",
    case all_in.counts when 0 then 0
    else round(coalesce(early_in.counts, 0)::numeric/all_in.counts::numeric * 100) end as "EarlyEntries"
from all_in
left join early_in on all_in.month = early_in.month;
end
-- $$ language plpgsql;

BEGIN;
CALL p17( );
FETCH ALL FROM "p17";
END;