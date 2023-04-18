-- for test --
ALTER SEQUENCE "p2p_ID_seq" RESTART WITH 33;
ALTER SEQUENCE "verter_ID_seq" RESTART WITH 27;
ALTER SEQUENCE "xp_ID_seq" RESTART WITH 14;
ALTER SEQUENCE "transferredpoints_ID_seq" RESTART WITH 19;
ALTER SEQUENCE "checks_ID_seq" RESTART WITH 17;

-- ex03 --
create or replace function fn_in_TP()
returns trigger
as $$
declare
	checked varchar :=(select "Peer" from Checks where Checks."ID"=new."Check" limit 1);
begin
if (new."State" = 'Start') then
	if (select "ID" from TransferredPoints where new."CheckingPeer"=TransferredPoints."CheckingPeer" and
			TransferredPoints."CheckedPeer"=checked) is not null then
		update TransferredPoints set "PointsAmount"="PointsAmount"+1
				where new."CheckingPeer"=TransferredPoints."CheckingPeer" and
					TransferredPoints."CheckedPeer"=checked;
	else
		insert into TransferredPoints ("CheckingPeer", "CheckedPeer", "PointsAmount")
		values (new."CheckingPeer", checked, 1);
	end if;
end if;
return new;
end
$$ language plpgsql;

create trigger tr_in_TP
after insert on P2P
for each row
execute function fn_in_TP();

-- ex01 --
create or replace procedure gen_insert_p2p(checked varchar, checking varchar,
 task_title varchar, status check_status, "time" time)
    language plpgsql
as
$$
begin
    if status = 'Start' then
        insert into checks ("Peer", "Task", "Date")
        values ((select "Nickname" from peers where "Nickname" = checked),
                (select "Title" from tasks where "Title" = task_title), now());
    end if;

    insert into P2P ("Check", "CheckingPeer", "State", "Time")
    values ( (select "ID" from Checks where "Peer" = checked order by 1 desc limit 1)
           , checking, status, "time");
end
$$;

ALTER SEQUENCE "p2p_ID_seq" RESTART WITH 33;
call gen_insert_p2p('Aboba', 'Amogus', 'C2_SimpleBashUtils', 'Start', '02:02:02'::time);
call gen_insert_p2p('Aboba', 'Amogus', 'C2_SimpleBashUtils', 'Failure', '02:02:03'::time);
-- call gen_insert_p2p('Amogus', 'Amogus', 'C2_SimpleBashUtils', 'Start', '04:02:03'::time);

--ex02--
create or replace procedure gen_insert_verter(checked varchar, task_title varchar, status check_status, "time" time)
    language plpgsql
as
$$
begin
    if status = 'Start' then
        insert into verter ("Check", "State", "Time")
        values ((select p2p."Check" from p2p
                join checks c on c."ID" = p2p."Check"
                where p2p."State" = 'Success' and "Peer" = checked and "Task" = task_title -- null не поставится
                order by 1 desc limit 1),
                status, "time");

    else
    insert into verter ("Check", "State", "Time")
        values ((select verter."Check" from verter where "State" = 'Start' order by 1 desc limit 1),
                status, "time");
    end if;
end
$$;


-- ex04 --
create or replace function fnc_xp_insert() returns trigger as
$$
begin
    if new."XPAmount" <= (select "MaxXP" from tasks
        join checks c on tasks."Title" = c."Task" where c."ID" = new."Check" limit 1) and
       (select "State" from verter where new."Check" = verter."Check" and "State" = 'Success') is not null
        or (select "State" from verter where new."Check" = verter."Check") is null and
           (select "State" from p2p where new."Check" = p2p."Check" and "State" = 'Success') is not null then
    return new;
    else
    RAISE EXCEPTION 'Invalid insert data';
    end if;
end
$$
language plpgsql;

create trigger trg_xp_insert
    before insert on xp
    for each row
    execute function fnc_xp_insert();

----------------tests------------------

create or replace procedure gen_insert_p2p_date(checked varchar, checking varchar,
 task_title varchar, status check_status, "date" date default now(), "time" time default now())
    language plpgsql
as
$$
begin
    if (select "ParentTask" from tasks where "Title" = task_title) is not null
        and (select count(*) from checks
            join xp x on checks."ID" = x."Check"
            join tasks t on t."ParentTask" = checks."Task"
            where "Peer" = checked and task_title = t."Title") = 0
    then RAISE EXCEPTION 'Invalid insert data'; end if;

    if status = 'Start' then
        insert into checks ("Peer", "Task", "Date")
        values ((select "Nickname" from peers where "Nickname" = checked),
                (select "Title" from tasks where "Title" = task_title), "date");
    end if;

    insert into P2P ("Check", "CheckingPeer", "State", "Time")
    values ( (select "ID" from Checks where "Peer" = checked order by 1 desc limit 1)
           , checking, status, "time");
end;
$$;


create or replace procedure create_check(checked varchar, checking varchar, task_title varchar,
fail_on varchar default 'no fail', "date" date default now(), "time" time default '00:10:02', timeInterval interval default '30 minutes')
as $$
begin
    --p2p
    call gen_insert_p2p_date(checked, checking, task_title, 'Start', "date", "time");
    if fail_on = 'p2p' then
        call gen_insert_p2p_date(checked, checking, task_title, 'Failure', "date", "time" + timeInterval);
        return ;
    end if;
    call gen_insert_p2p_date(checked, checking, task_title, 'Success', "date", "time" + timeInterval);

    --verter
    if fail_on != 'no vertor, but success' then
    call gen_insert_verter(checked, task_title, 'Start', "time" + timeInterval + '1 minutes');
    if fail_on = 'verter' then
    call gen_insert_verter(checked,task_title, 'Failure', "time" + timeInterval + '2 minutes');
    return ;
    end if;
    end if;
    if fail_on != 'no vertor, but success' then
    call gen_insert_verter(checked,task_title, 'Success', "time" + timeInterval + '2 minutes');
    end if;

    --xp
    insert into xp ("Check", "XPAmount") values ((select "ID" from checks where "Peer" = checked and "Task" = task_title order by 1 desc limit 1),
                                                 (select "MaxXP" from tasks where "Title" = task_title));
end
$$
language plpgsql;

-- initial
call create_check('bmcgrail4', 'esminus3', 'C2_SimpleBashUtils',
            'no fail', (now() + '2 days')::date,'05:02:02', '28 minutes');
call create_check('bmcgrail4', 'acraigheid0', 'C3_s21_string',
            'no fail', (now() + '3 day')::date,'07:12:02', '40 minutes');
call create_check('bmcgrail4', 'areilly3', 'C4_s21_math',
            'no fail', (now() + '5 days')::date,'18:22:02', '1 hour');
call create_check('bmcgrail4', 'sbirks1', 'C5_s21_decimal',
            'p2p', (now() + '6 days')::date,'20:22:02', '1 hour');
call create_check('kapperley2', 'areilly3', 'C2_SimpleBashUtils',
            'no fail', (now() + '6 days')::date,'20:22:02', '2 hours');


-- for ex10 --
call create_check('yaaz7', 'sbirks1', 'C2_SimpleBashUtils',
            'no fail', ('2023-02-28')::date,'20:22:02', '2 hours');
call create_check('yaaz7', 'acraigheid0', 'C3_s21_string',
            'no fail', ('2024-02-28')::date,'18:40:02', '1 hours');
call create_check('arbuzhochu8', 'yaaz7', 'C2_SimpleBashUtils',
            'verter', ('2023-02-28')::date,'20:22:02', '2 hours');
call create_check('kapperley2', 'yaaz7', 'C3_s21_string',
            'no fail', ('2023-08-04')::date,'15:22:02', '2 hours');
call create_check('esminus3', 'bmcgrail4', 'C5_s21_decimal',
            'no fail', ('2023-01-13')::date,'10:22:02', '1 hours');
call create_check('erjan1', 'kapperley2', 'C2_SimpleBashUtils',
            'p2p', ('2023-06-22')::date,'5:22:02', '1 hours');
call create_check('erjan1', 'bmcgrail4', 'C2_SimpleBashUtils',
            'no fail', ('2023-06-22')::date,'18:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'C3_s21_string',
            'no vertor, but success', ('2023-06-22')::date,'22:22:02', '1 hours');

-- for ex07 --
call create_check('esminus3', 'sbirks1', 'C6_s21_matrix',
            'no fail', (now() + '1 days')::date,'20:22:02', '1 hours');
call create_check('esminus3', 'acraigheid0', 'C7_SmartCalc_v1_0',
            'no fail', (now() + '2 days')::date,'20:22:02', '1 hours');
call create_check('esminus3', 'Luckyboy', 'C8_3DViewer_v1_0',
            'no fail', (now() + '3 days')::date,'20:22:02', '2 hours');

call create_check('esminus3', 'Luckyboy', 'C8_3DViewer_v1_0',
            'no fail', (now() + '3 days')::date,'20:22:02', '2 hours');


call create_check('esminus3', 'yaaz7', 'CPP1_s21_matrix',
            'no fail', (now() + '4 days')::date,'20:22:02', '2 hours');

call create_check('erjan1', 'sbirks1', 'C4_s21_math',
            'no vertor, but success', ('2023-06-23')::date,'22:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'C5_s21_decimal',
            'no vertor, but success', ('2023-06-24')::date,'22:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'C6_s21_matrix',
            'no vertor, but success', ('2023-06-25')::date,'22:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'C7_SmartCalc_v1_0',
            'no vertor, but success', ('2023-06-26')::date,'22:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'C8_3DViewer_v1_0',
            'no vertor, but success', ('2023-06-27')::date,'22:22:02', '1 hours');
call create_check('erjan1', 'sbirks1', 'CPP1_s21_matrix',
            'no vertor, but success', ('2023-06-29')::date,'22:22:02', '1 hours');